# Fluree Semantic Web Developer Guide

## Overview

Fluree is a native RDF graph database that fully embraces Semantic Web
standards. This guide demonstrates how to use Fluree with familiar technologies
like Turtle, SPARQL, SHACL, and OWL.

## Table of Contents

- [Turtle Support](#turtle-support)
- [SPARQL Queries](#sparql-queries)
- [SHACL Validation](#shacl-validation)
- [OWL Reasoning](#owl-reasoning)
- [Cross-Database Queries](#cross-database-queries)
- [Complete Examples](#complete-examples)

## Turtle Support

Fluree natively supports Turtle (Terse RDF Triple Language) for data insertion
and serialization.

### Inserting Data with Turtle

```clojure
(require '[fluree.db.api :as fluree])

;; Create connection and ledger
(def conn @(fluree/connect-memory))
(def ledger @(fluree/create conn "semantic-web-example"))

;; Insert data using Turtle format
(def db @(fluree/insert (fluree/db ledger)
                        "@prefix ex: <http://example.org/> .
                         @prefix foaf: <http://xmlns.com/foaf/0.1/> .
                         @prefix schema: <http://schema.org/> .
                         @prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
                         @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
                         
                         ex:alice a foaf:Person ;
                                  foaf:name \"Alice\" ;
                                  foaf:age 30 ;
                                  foaf:knows ex:bob ;
                                  schema:email \"alice@example.org\" .
                         
                         ex:bob a foaf:Person ;
                                foaf:name \"Bob\" ;
                                foaf:age 25 ;
                                schema:email \"bob@example.org\" .
                         
                         ex:CompanyA a schema:Organization ;
                                     schema:name \"Company A\" ;
                                     schema:employee ex:alice, ex:bob ."
                        {:format :turtle}))

;; Commit the changes
@(fluree/commit! ledger db)
```

### Complex Turtle with Blank Nodes

```clojure
(def db @(fluree/insert (fluree/db ledger)
                        "@prefix ex: <http://example.org/> .
                         @prefix geo: <http://www.w3.org/2003/01/geo/wgs84_pos#> .
                         @prefix schema: <http://schema.org/> .
                         
                         ex:alice schema:address [
                           a schema:PostalAddress ;
                           schema:streetAddress \"123 Main St\" ;
                           schema:addressLocality \"Springfield\" ;
                           schema:postalCode \"12345\" ;
                           geo:lat 39.781 ;
                           geo:long -89.650
                         ] ."
                        {:format :turtle}))
```

## SPARQL Queries

Fluree supports SPARQL 1.1 Query Language for complex graph pattern matching.

### Basic SPARQL SELECT

```clojure
(def results @(fluree/query db
                           "PREFIX foaf: <http://xmlns.com/foaf/0.1/>
                            PREFIX schema: <http://schema.org/>
                            
                            SELECT ?person ?name ?email
                            WHERE {
                              ?person a foaf:Person ;
                                      foaf:name ?name ;
                                      schema:email ?email .
                            }
                            ORDER BY ?name"
                           {:format :sparql}))
```

### SPARQL with OPTIONAL and FILTER

```clojure
(def results @(fluree/query db
                           "PREFIX foaf: <http://xmlns.com/foaf/0.1/>
                            PREFIX schema: <http://schema.org/>
                            
                            SELECT ?person ?name ?age ?email
                            WHERE {
                              ?person a foaf:Person ;
                                      foaf:name ?name .
                              OPTIONAL { ?person foaf:age ?age }
                              OPTIONAL { ?person schema:email ?email }
                              FILTER (!BOUND(?age) || ?age >= 18)
                            }"
                           {:format :sparql}))
```

### SPARQL Aggregation

```clojure
(def results @(fluree/query db
                           "PREFIX schema: <http://schema.org/>
                            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
                            
                            SELECT ?company (COUNT(?employee) AS ?employeeCount) 
                                   (AVG(?age) AS ?avgAge)
                            WHERE {
                              ?company a schema:Organization ;
                                       schema:employee ?employee .
                              ?employee foaf:age ?age .
                            }
                            GROUP BY ?company
                            HAVING (COUNT(?employee) > 1)"
                           {:format :sparql}))
```

### SPARQL CONSTRUCT

```clojure
(def constructed @(fluree/query db
                               "PREFIX foaf: <http://xmlns.com/foaf/0.1/>
                                PREFIX ex: <http://example.org/>
                                
                                CONSTRUCT {
                                  ?person ex:hasConnection ?friend .
                                  ?friend ex:connectedTo ?person .
                                }
                                WHERE {
                                  ?person foaf:knows ?friend .
                                }"
                               {:format :sparql}))
```

## SPARQL UPDATE

Fluree supports SPARQL 1.1 Update operations for data modification.

### INSERT DATA

```clojure
;; Insert new triples using SPARQL UPDATE
(def db @(fluree/update (fluree/db ledger)
                        "PREFIX ex: <http://example.org/>
                         PREFIX foaf: <http://xmlns.com/foaf/0.1/>
                         PREFIX schema: <http://schema.org/>
                         
                         INSERT DATA {
                           ex:charlie a foaf:Person ;
                                     foaf:name \"Charlie\" ;
                                     foaf:age 28 ;
                                     schema:email \"charlie@example.org\" .
                         }"
                        {:format :sparql}))

;; Or insert and commit atomically
@(fluree/update! ledger
                 {"ledger" "my-ledger"
                  "update" "PREFIX ex: <http://example.org/>
                           PREFIX foaf: <http://xmlns.com/foaf/0.1/>
                           
                           INSERT DATA {
                             ex:david a foaf:Person ;
                                     foaf:name \"David\" .
                           }"}
                 {:format :sparql})
```

### DELETE DATA

```clojure
;; Delete specific triples
(def db @(fluree/update (fluree/db ledger)
                        "PREFIX ex: <http://example.org/>
                         PREFIX foaf: <http://xmlns.com/foaf/0.1/>
                         
                         DELETE DATA {
                           ex:alice foaf:age 30 .
                         }"
                        {:format :sparql}))
```

### DELETE WHERE

```clojure
;; Delete based on pattern matching
(def db @(fluree/update (fluree/db ledger)
                        "PREFIX foaf: <http://xmlns.com/foaf/0.1/>
                         
                         DELETE WHERE {
                           ?person foaf:age ?age .
                           FILTER (?age < 18)
                         }"
                        {:format :sparql}))
```

### DELETE/INSERT WHERE

```clojure
;; Update data using DELETE/INSERT
(def db @(fluree/update (fluree/db ledger)
                        "PREFIX foaf: <http://xmlns.com/foaf/0.1/>
                         PREFIX ex: <http://example.org/>
                         
                         WITH ex:graph1
                         DELETE { ?person foaf:givenName \"Bill\" }
                         INSERT { ?person foaf:givenName \"William\" }
                         WHERE {
                           ?person foaf:givenName \"Bill\" .
                         }"
                        {:format :sparql}))
```

### SPARQL UPDATE with USING

```clojure
;; Specify source graph for WHERE patterns
(def db @(fluree/update (fluree/db ledger)
                        "PREFIX foaf: <http://xmlns.com/foaf/0.1/>
                         
                         DELETE { ?person foaf:status \"active\" }
                         INSERT { ?person foaf:status \"inactive\" }
                         USING <http://example.org/employees>
                         WHERE {
                           ?person foaf:age ?age .
                           FILTER (?age >= 65)
                         }"
                        {:format :sparql}))
```

### Notes on SPARQL UPDATE Support

- All standard SPARQL UPDATE operations are supported: INSERT DATA, DELETE DATA,
    DELETE WHERE, DELETE/INSERT WHERE
- The WITH clause is supported for specifying the target graph
- The USING clause is supported (limited to one USING clause per update)
- An UPDATE operation can only create triples for a single graph, using WITH or a single USING clause
- USING NAMED is not supported
- Updates can be staged with `fluree/update` or committed atomically with
    `fluree/update!`
- Use the `{:format :sparql}` option to indicate SPARQL UPDATE syntax

## SHACL Validation

Fluree supports W3C SHACL (Shapes Constraint Language) for data validation.

### Basic SHACL Shape

```clojure
;; Define a SHACL shape for Person validation
(def person-shape
  "@prefix sh: <http://www.w3.org/ns/shacl#> .
   @prefix foaf: <http://xmlns.com/foaf/0.1/> .
   @prefix schema: <http://schema.org/> .
   @prefix ex: <http://example.org/> .
   
   ex:PersonShape a sh:NodeShape ;
     sh:targetClass foaf:Person ;
     sh:property [
       sh:path foaf:name ;
       sh:minCount 1 ;
       sh:maxCount 1 ;
       sh:datatype xsd:string ;
       sh:minLength 1 ;
       sh:maxLength 100 ;
       sh:message \"Person must have exactly one name between 1-100 characters\"
     ] ;
     sh:property [
       sh:path foaf:age ;
       sh:datatype xsd:integer ;
       sh:minInclusive 0 ;
       sh:maxInclusive 150 ;
       sh:message \"Age must be between 0 and 150\"
     ] ;
     sh:property [
       sh:path schema:email ;
       sh:nodeKind sh:Literal ;
       sh:pattern \"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$\" ;
       sh:message \"Invalid email format\"
     ] .")

;; Insert the SHACL shape
(def db-with-shapes @(fluree/insert db person-shape {:format :turtle}))
```

### Advanced SHACL Constraints

```clojure
(def advanced-shapes
  "@prefix sh: <http://www.w3.org/ns/shacl#> .
   @prefix schema: <http://schema.org/> .
   @prefix ex: <http://example.org/> .
   
   # Organization shape with complex constraints
   ex:OrganizationShape a sh:NodeShape ;
     sh:targetClass schema:Organization ;
     sh:property [
       sh:path schema:employee ;
       sh:minCount 1 ;
       sh:node ex:PersonShape ;  # Employees must conform to PersonShape
       sh:message \"Organization must have at least one valid employee\"
     ] ;
     sh:property [
       sh:path schema:name ;
       sh:minCount 1 ;
       sh:uniqueLang true ;  # Only one name per language
     ] ;
     sh:sparql [
       # Custom SPARQL constraint
       a sh:SPARQLConstraint ;
       sh:message \"Organization must not employ anyone under 18\" ;
       sh:select \"\"\"
         PREFIX schema: <http://schema.org/>
         PREFIX foaf: <http://xmlns.com/foaf/0.1/>
         SELECT $this WHERE {
           $this schema:employee ?employee .
           ?employee foaf:age ?age .
           FILTER (?age < 18)
         }
       \"\"\"
     ] .")
```

## OWL Reasoning

Fluree includes an OWL2-RL reasoner for inferring new facts from ontologies.

### Basic OWL Ontology

```clojure
;; Define an OWL ontology
(def ontology
  "@prefix owl: <http://www.w3.org/2002/07/owl#> .
   @prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
   @prefix foaf: <http://xmlns.com/foaf/0.1/> .
   @prefix schema: <http://schema.org/> .
   @prefix ex: <http://example.org/> .
   
   # Class hierarchy
   schema:Person rdfs:subClassOf schema:Thing .
   ex:Employee rdfs:subClassOf schema:Person .
   ex:Manager rdfs:subClassOf ex:Employee .
   
   # Property definitions
   ex:manages a owl:ObjectProperty ;
             rdfs:domain ex:Manager ;
             rdfs:range ex:Employee ;
             owl:inverseOf ex:managedBy .
   
   ex:managedBy a owl:ObjectProperty ;
                rdfs:domain ex:Employee ;
                rdfs:range ex:Manager .
   
   # Transitive property
   ex:reportsTo a owl:ObjectProperty ;
                a owl:TransitiveProperty ;
                rdfs:domain ex:Employee ;
                rdfs:range ex:Employee .
   
   # Functional property (can have at most one value)
   ex:employeeId a owl:FunctionalProperty ;
                 rdfs:domain ex:Employee ;
                 rdfs:range xsd:string .")

;; Insert the ontology
(def db-with-ontology @(fluree/insert db ontology {:format :turtle}))

;; Apply OWL reasoning
(def reasoned-db @(fluree/reason db-with-ontology :owl2rl))

;; Query for inferred facts
(def inferred-facts @(fluree/reasoned-facts reasoned-db))
```

### Complex OWL Reasoning Example

```clojure
;; Add data that will trigger reasoning
(def db-with-data @(fluree/insert db-with-ontology
                                  "@prefix ex: <http://example.org/> .
                                   @prefix foaf: <http://xmlns.com/foaf/0.1/> .
                                   
                                   ex:alice a ex:Manager ;
                                           foaf:name \"Alice\" ;
                                           ex:manages ex:bob, ex:charlie ;
                                           ex:employeeId \"MGR001\" .
                                   
                                   ex:bob a ex:Employee ;
                                         foaf:name \"Bob\" ;
                                         ex:reportsTo ex:alice ;
                                         ex:employeeId \"EMP001\" .
                                   
                                   ex:charlie a ex:Employee ;
                                             foaf:name \"Charlie\" ;
                                             ex:reportsTo ex:bob ;
                                             ex:employeeId \"EMP002\" ."
                                  {:format :turtle}))

;; Apply reasoning
(def reasoned @(fluree/reason db-with-data :owl2rl))

;; Query for transitive relationships
(def results @(fluree/query reasoned
                           "PREFIX ex: <http://example.org/>
                            PREFIX foaf: <http://xmlns.com/foaf/0.1/>
                            
                            SELECT ?employee ?manager ?managerName
                            WHERE {
                              ?employee ex:reportsTo ?manager .
                              ?manager foaf:name ?managerName .
                            }
                            ORDER BY ?employee"
                           {:format :sparql}))
;; This will show that Charlie reports to both Bob (direct) and Alice (inferred)
```

### OWL Property Chains

```clojure
(def property-chain-ontology
  "@prefix owl: <http://www.w3.org/2002/07/owl#> .
   @prefix ex: <http://example.org/> .
   
   ex:hasParent a owl:ObjectProperty .
   ex:hasBrother a owl:ObjectProperty .
   ex:hasUncle a owl:ObjectProperty ;
              owl:propertyChainAxiom (ex:hasParent ex:hasBrother) .")
```

## Cross-Database Queries

Fluree supports querying across multiple databases/ledgers using SPARQL FROM and
FROM NAMED clauses.

### Query Across Multiple Fluree Databases

```clojure
;; Create multiple ledgers with related data
(def conn @(fluree/connect-memory))
(def people-ledger @(fluree/create conn "people"))
(def projects-ledger @(fluree/create conn "projects"))

;; Insert data into people ledger
(def people-db @(fluree/insert! people-ledger (fluree/db people-ledger)
                                "@prefix foaf: <http://xmlns.com/foaf/0.1/> .
                                 @prefix ex: <http://example.org/> .
                                 
                                 ex:alice a foaf:Person ;
                                         foaf:name \"Alice\" ;
                                         ex:expertise \"Machine Learning\" .
                                 
                                 ex:bob a foaf:Person ;
                                       foaf:name \"Bob\" ;
                                       ex:expertise \"Database Systems\" ."
                                {:format :turtle}))

;; Insert data into projects ledger
(def projects-db @(fluree/insert! projects-ledger (fluree/db projects-ledger)
                                  "@prefix ex: <http://example.org/> .
                                   @prefix schema: <http://schema.org/> .
                                   
                                   ex:project1 a ex:Project ;
                                              schema:name \"AI Research\" ;
                                              ex:lead ex:alice ;
                                              ex:member ex:bob ."
                                  {:format :turtle}))

;; Query across both ledgers using FROM
(def cross-ledger-results
  @(fluree/query people-db
                 "PREFIX foaf: <http://xmlns.com/foaf/0.1/>
                  PREFIX ex: <http://example.org/>
                  PREFIX schema: <http://schema.org/>
                  
                  SELECT ?person ?name ?project ?projectName
                  FROM <people>
                  FROM <projects>
                  WHERE {
                    ?person a foaf:Person ;
                            foaf:name ?name .
                    ?project ex:member ?person ;
                            schema:name ?projectName .
                  }"
                 {:format :sparql}))
```

### Using FROM NAMED for Graph-Specific Queries

```clojure
;; Query with named graphs
(def named-graph-results
  @(fluree/query people-db
                 "PREFIX foaf: <http://xmlns.com/foaf/0.1/>
                  PREFIX ex: <http://example.org/>
                  
                  SELECT ?person ?name ?graph
                  FROM NAMED <people>
                  FROM NAMED <projects>
                  WHERE {
                    GRAPH ?graph {
                      ?person foaf:name ?name .
                    }
                  }"
                 {:format :sparql}))
```

## Access Control with Fluree Policies

While SHACL is used for data validation, access control in Fluree is handled
through its native policy system using JSON-LD queries.

### Basic Policy Example

```clojure
;; Define a policy that restricts access based on user properties
(def access-policy
  {"@context" {"f" "https://ns.flur.ee/ledger#"
               "ex" "http://example.org/"}
   "@id" "ex:EmployeeAccessPolicy"
   "@type" "f:AccessPolicy"
   "f:targetClass" "ex:EmployeeData"
   "f:action" ["f:view" "f:modify"]
   "f:query" {"@context" {"ex" "http://example.org/"}
              "select" "?data"
              "where" {"@id" "?data"
                       "ex:department" "?dept"
                       "filter" ["=" "?dept" "?userDepartment"]}}})
```

## Complete Examples

### Example 1: Knowledge Graph with SHACL Validation

```clojure
(require '[fluree.db.api :as fluree])

;; Create a knowledge graph with validation
(defn create-validated-knowledge-graph []
  (let [conn @(fluree/connect-memory)
        ledger @(fluree/create conn "knowledge-graph")]
    
    ;; 1. Insert SHACL shapes
    (def shapes
      "@prefix sh: <http://www.w3.org/ns/shacl#> .
       @prefix ex: <http://example.org/> .
       @prefix schema: <http://schema.org/> .
       
       ex:ArticleShape a sh:NodeShape ;
         sh:targetClass ex:Article ;
         sh:property [
           sh:path schema:headline ;
           sh:minCount 1 ;
           sh:maxLength 200
         ] ;
         sh:property [
           sh:path schema:author ;
           sh:minCount 1 ;
           sh:class schema:Person
         ] ;
         sh:property [
           sh:path schema:datePublished ;
           sh:minCount 1 ;
           sh:datatype xsd:date
         ] .")
    
    ;; 2. Insert ontology
    (def ontology
      "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
       @prefix ex: <http://example.org/> .
       
       ex:ScientificArticle rdfs:subClassOf ex:Article .
       ex:NewsArticle rdfs:subClassOf ex:Article .
       ex:peerReviewed rdfs:subPropertyOf schema:additionalProperty .")
    
    ;; 3. Insert data
    (def data
      "@prefix ex: <http://example.org/> .
       @prefix schema: <http://schema.org/> .
       
       ex:article1 a ex:ScientificArticle ;
         schema:headline \"Quantum Computing Breakthrough\" ;
         schema:author ex:drSmith ;
         schema:datePublished \"2024-01-15\"^^xsd:date ;
         ex:peerReviewed true .
       
       ex:drSmith a schema:Person ;
         schema:name \"Dr. Jane Smith\" ;
         schema:affiliation \"MIT\" .")
    
    ;; Insert all data
    (let [db1 @(fluree/insert (fluree/db ledger) shapes {:format :turtle})
          db2 @(fluree/insert db1 ontology {:format :turtle})
          db3 @(fluree/insert db2 data {:format :turtle})]
      @(fluree/commit! ledger db3))
    
    ;; Apply reasoning
    (def reasoned @(fluree/reason (fluree/db ledger) :owl2rl))
    
    ;; Query with SPARQL
    @(fluree/query reasoned
                   "PREFIX ex: <http://example.org/>
                    PREFIX schema: <http://schema.org/>
                    
                    SELECT ?article ?type ?headline ?author
                    WHERE {
                      ?article a ?type ;
                               schema:headline ?headline ;
                               schema:author ?author .
                      ?type rdfs:subClassOf* ex:Article .
                    }"
                   {:format :sparql})))
```

### Example 2: Temporal Semantic Web Data

```clojure
;; Track changes to RDF data over time
(defn temporal-rdf-example []
  (let [conn @(fluree/connect-memory)
        ledger @(fluree/create conn "temporal-rdf")]
    
    ;; Initial state
    (def t1-db @(fluree/insert! ledger (fluree/db ledger)
                                "@prefix foaf: <http://xmlns.com/foaf/0.1/> .
                                 @prefix ex: <http://example.org/> .
                                 
                                 ex:project1 a ex:Project ;
                                            ex:status \"active\" ;
                                            ex:lead ex:alice ."
                                {:format :turtle}))
    
    ;; Update status
    (Thread/sleep 1000)
    (def t2-db @(fluree/update! ledger t1-db
                                "@prefix ex: <http://example.org/> .
                                 
                                 ex:project1 ex:status \"completed\" ."
                                {:format :turtle}))
    
    ;; Query history with SPARQL
    @(fluree/query (fluree/db ledger)
                   "PREFIX ex: <http://example.org/>
                    PREFIX f: <https://ns.flur.ee/ledger#>
                    
                    SELECT ?time ?status
                    WHERE {
                      GRAPH ?commit {
                        ex:project1 ex:status ?status .
                      }
                      ?commit f:time ?time .
                    }
                    ORDER BY ?time"
                   {:format :sparql})))
```

## Best Practices

### 1. Namespace Management
Always define clear namespace prefixes and use consistent URIs across your data:

```turtle
@prefix : <http://example.org/vocab/> .
@prefix data: <http://example.org/data/> .
@prefix owl: <http://www.w3.org/2002/07/owl#> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .
```

### 2. SHACL Design Patterns
- Use SHACL for data validation
- Define reusable shapes with type sh:NodeShape
- Leverage SPARQL-based constraints for complex validation rules

### 3. Reasoning Performance
- Apply reasoning selectively using graph patterns
- Pre-compute common inferences
- Use property paths in SPARQL when possible, noting that support is limited to sequences of predicate/inverse path segments and single segments of one-or-more, zero-or-more, or zero-or-one paths (negated paths not supported)

### 4. Cross-Database Query Guidelines
- Use descriptive ledger names for clarity in FROM clauses
- Consider performance implications when joining across large databases
- Use LIMIT clauses to control result set sizes

## Additional Resources

- [W3C SPARQL 1.1 Specification](https://www.w3.org/TR/sparql11-query/)
- [W3C SHACL Specification](https://www.w3.org/TR/shacl/)
- [W3C OWL 2 Primer](https://www.w3.org/TR/owl2-primer/)
- [Turtle Specification](https://www.w3.org/TR/turtle/)
- [JSON-LD Specification](https://www.w3.org/TR/json-ld11/)

## Conclusion

Fluree provides comprehensive support for Semantic Web standards, making it an
ideal choice for building knowledge graphs, linked data applications, and
semantic web services. The combination of native RDF storage, SPARQL querying,
SHACL validation, and OWL reasoning provides a complete platform for semantic
web development.