# Fluree Clojure Developer Guide

## Overview

Fluree brings semantic web capabilities to the Clojure ecosystem while
maintaining the developer experience you expect. By combining immutable time-
travel databases with RDF, SPARQL, SHACL, and OWL, Fluree enables building
knowledge graphs and linked data applications that seamlessly integrate with the
broader semantic web ecosystem.

If you're coming from Datomic or XTDB, you'll find Fluree shares many familiar
concepts: immutable data, time travel queries, and a Clojure-native API.
However, Fluree extends these with semantic web standards, a built-in reasoning
engine, and cryptographic data integrity - providing a unique combination of
graph database flexibility with cryptographic verifiability.

## Why Fluree for Clojure Developers

### Familiar Concepts
- **Immutable, append-only** database like Datomic
- **Time travel** queries across all historical states
- **Datalog-style** pattern matching in queries
- **Schema-optional** flexibility like XTDB
- **ACID transactions** with strong consistency

### Unique Advantages
- **Semantic Web Native**: Built on RDF with JSON-LD, enabling linked data
    applications
- **Multi-Query Languages**: Same data queryable via JSON-LD queries and SPARQL
- **Built-in Reasoning**: OWL2-RL and Datalog inference engines
- **Standards-Based Validation**: W3C SHACL for data constraints
- **Cryptographic Integrity**: Cryptographically verifiable ledger with provable
    commits
- **Fine-Grained Policies**: Data-centric access control, not just connection-
    based
- **Sophisticated Transactions**: Pattern-based updates with WHERE clauses (no
    compare-and-swap retries)
- **ClojureScript Compatible**: Full support for browser and Node.js
    environments
- **Consensus-Based Clustering**: Built-in support for redundant servers via
    Raft consensus using the [com.fluree/raft](https://github.com/fluree/raft) library and [Fluree Server](https://github.com/fluree/server) for HTTP API
    clustering

## Table of Contents

- [Getting Started](#getting-started)
- [Data Operations](#data-operations)
- [Querying](#querying)
- [Time Travel](#time-travel)
- [Schema and Validation](#schema-and-validation)
- [Reasoning](#reasoning)
- [Policies and Access Control](#policies-and-access-control)
- [Advanced Features](#advanced-features)

## Getting Started

### Basic Setup

```clojure
(ns myapp.db
  (:require [fluree.db.api :as fluree]))

;; Define a keyword-based context for Clojure-idiomatic usage
(def default-context
  {:id "@id"
   :type "@type"
   :schema "http://schema.org/"
   :ex "http://example.org/"
   :rdf "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
   :rdfs "http://www.w3.org/2000/01/rdf-schema#"
   :xsd "http://www.w3.org/2001/XMLSchema#"})

;; Create an in-memory connection
(def conn @(fluree/connect-memory))

;; Create a ledger
(def ledger @(fluree/create conn "my-app"))
```

## Data Operations

### Insert Data

```clojure
;; Insert new entities using keywords
(def db1 @(fluree/insert (fluree/db ledger)
                         [{:id :ex/alice
                           :type :schema/Person
                           :schema/name "Alice"
                           :schema/email "alice@example.org"
                           :schema/age 30
                           :ex/role :ex/developer}
                          {:id :ex/bob
                           :type :schema/Person
                           :schema/name "Bob"
                           :schema/email "bob@example.org"
                           :schema/age 25
                           :ex/role :ex/designer}]
                         {:context default-context}))

;; Commit the staged changes
@(fluree/commit! ledger db1)

;; Or insert and commit atomically
@(fluree/insert! ledger (fluree/db ledger)
                 [{:id :ex/project1
                   :type :ex/Project
                   :schema/name "Knowledge Graph"
                   :ex/lead :ex/alice
                   :ex/members [:ex/alice :ex/bob]
                   :ex/status :ex/active}]
                 {:context default-context})
```

### Update Data

```clojure
;; Stage updates to existing entities
(def db2 @(fluree/update (fluree/db ledger)
                         [{:id :ex/alice
                           :schema/age 31
                           :ex/skills [:ex/clojure :ex/rdf :ex/sparql]}
                          {:id :ex/project1
                           :ex/status :ex/completed}]
                         {:context default-context}))

;; Commit updates
@(fluree/commit! ledger db2)

;; Or update atomically
@(fluree/update! ledger (fluree/db ledger)
                 [{:id :ex/bob
                   :ex/skills [:ex/ui :ex/ux :ex/css]}]
                 {:context default-context})
```

### Upsert Data

```clojure
;; Insert or update based on :id
@(fluree/upsert! ledger (fluree/db ledger)
                 [{:id :ex/charlie
                   :type :schema/Person
                   :schema/name "Charlie"
                   :schema/email "charlie@example.org"
                   :ex/role :ex/manager}
                  {:id :ex/alice  ; Updates existing Alice
                   :ex/department :ex/engineering}]
                 {:context default-context})
```

## Querying

### Basic Analytical Queries

```clojure
;; Simple select query with symbols for variables
(def results 
  @(fluree/query (fluree/db ledger)
                 {:context default-context
                  :select '[?person ?name ?age]
                  :where '{:id ?person
                           :type :schema/Person
                           :schema/name ?name
                           :schema/age ?age}}))

;; Query with filtering
(def adults
  @(fluree/query (fluree/db ledger)
                 {:context default-context
                  :select '[?person ?name]
                  :where [{:id '?person
                           :type :schema/Person
                           :schema/name '?name
                           :schema/age '?age}
                          {:filter '(>= ?age 18)}]}))

;; Graph crawl with nested selection
(def person-details
  @(fluree/query (fluree/db ledger)
                 {:context default-context
                  :select '{?person [:schema/name 
                                     :schema/email
                                     :schema/age
                                     {:ex/skills [:*]}]}
                  :where '{:id ?person
                           :type :schema/Person}}))
```

### Select One

```clojure
;; Return single result
(def alice-data
  @(fluree/query (fluree/db ledger)
                 {:context default-context
                  :select-one '[?name ?email ?age]
                  :where '{:id :ex/alice
                           :schema/name ?name
                           :schema/email ?email
                           :schema/age ?age}}))
```

### Aggregation Queries

```clojure
;; Count and average
(def stats
  @(fluree/query (fluree/db ledger)
                 {:context default-context
                  :select '[?role (count ?person) (avg ?age)]
                  :where '{:id ?person
                           :type :schema/Person
                           :ex/role ?role
                           :schema/age ?age}
                  :group-by '?role}))

;; With having clause
(def large-teams
  @(fluree/query (fluree/db ledger)
                 {:context default-context
                  :select '[?project ?lead (count ?member)]
                  :where '{:id ?project
                           :type :ex/Project
                           :ex/lead ?lead
                           :ex/members ?member}
                  :group-by '[?project ?lead]
                  :having '(> (count ?member) 5)}))
```

### Complex Queries

```clojure
;; Optional patterns
(def people-with-optional-age
  @(fluree/query (fluree/db ledger)
                 {:context default-context
                  :select '[?person ?name ?age]
                  :where [{:id '?person
                           :type :schema/Person
                           :schema/name '?name}
                          {:optional [{:id '?person
                                       :schema/age '?age}]}]}))

;; Union patterns
(def all-entities
  @(fluree/query (fluree/db ledger)
                 {:context default-context
                  :select '[?entity ?name ?type]
                  :where [{:union [[{:id '?entity
                                     :type :schema/Person
                                     :schema/name '?name
                                     :bind [['?type "Person"]]}]
                                   [{:id '?entity
                                     :type :ex/Project
                                     :schema/name '?name
                                     :bind [['?type "Project"]]}]]}]}))

;; Subqueries
(def top-contributors
  @(fluree/query (fluree/db ledger)
                 {:context default-context
                  :select '[?person ?name ?project-count]
                  :where [{:id '?person
                           :schema/name '?name}
                          {:query {:select '[?person (count ?project)]
                                   :where '{:id ?project
                                            :ex/members ?person}
                                   :group-by '?person
                                   :bind [['?project-count '(count ?project)]]}}]}))
```

## Time Travel

### Historical Queries

```clojure
;; Query at specific time
(def historical-db 
  @(fluree/history ledger {:at "2024-01-01T00:00:00.000Z"}))

(def past-data
  @(fluree/query historical-db
                 {:context default-context
                  :select '[?person ?age]
                  :where '{:id ?person
                           :schema/age ?age}}))

;; Query entity history
(def alice-history
  @(fluree/history ledger 
                   {:context default-context
                    :at :latest
                    :commit-details true
                    :t {:from 1}}
                   {:id :ex/alice}))

;; Compare states across time
(defn compare-states [t1 t2]
  (let [db1 @(fluree/history ledger {:at t1})
        db2 @(fluree/history ledger {:at t2})
        q {:context default-context
           :select '[?person ?age]
           :where '{:id ?person
                    :schema/age ?age}}]
    {:time1 @(fluree/query db1 q)
     :time2 @(fluree/query db2 q)}))
```

## Schema and Validation

### Define Schemas with SHACL

```clojure
;; Define person schema
(def person-schema
  [{:id :ex/PersonShape
    :type [:sh/NodeShape]
    :sh/targetClass :schema/Person
    :sh/property [{:sh/path :schema/name
                   :sh/minCount 1
                   :sh/maxCount 1
                   :sh/datatype :xsd/string}
                  {:sh/path :schema/age
                   :sh/datatype :xsd/integer
                   :sh/minInclusive 0
                   :sh/maxInclusive 150}
                  {:sh/path :schema/email
                   :sh/pattern "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"}]}])

;; Insert schema
@(fluree/insert! ledger (fluree/db ledger)
                 person-schema
                 {:context (merge default-context
                                  {:sh "http://www.w3.org/ns/shacl#"})})
```

### Schema Evolution

```clojure
;; Add new optional properties
@(fluree/update! ledger (fluree/db ledger)
                 [{:id :ex/PersonShape
                   :sh/property {:sh/path :ex/department
                                 :sh/class :ex/Department
                                 :sh/minCount 0}}]
                 {:context (merge default-context
                                  {:sh "http://www.w3.org/ns/shacl#"})})
```

## Reasoning

### OWL Reasoning

```clojure
;; Define ontology
(def ontology
  [{:id :schema/Person
    :rdfs/subClassOf :schema/Thing}
   {:id :ex/Employee
    :rdfs/subClassOf :schema/Person}
   {:id :ex/Manager
    :rdfs/subClassOf :ex/Employee}
   {:id :ex/manages
    :type :owl/ObjectProperty
    :rdfs/domain :ex/Manager
    :rdfs/range :ex/Employee
    :owl/inverseOf :ex/managedBy}])

;; Insert ontology
@(fluree/insert! ledger (fluree/db ledger)
                 ontology
                 {:context (merge default-context
                                  {:owl "http://www.w3.org/2002/07/owl#"})})

;; Apply reasoning
(def reasoned-db @(fluree/reason (fluree/db ledger) :owl2rl))

;; Query inferred facts
(def managers
  @(fluree/query reasoned-db
                 {:context default-context
                  :select '[?person ?name]
                  :where '{:id ?person
                           :type :ex/Manager  ; Inferred from :ex/manages property
                           :schema/name ?name}}))

;; Get all inferred facts
(def inferences @(fluree/reasoned-facts reasoned-db))
```

### Datalog Rules

```clojure
;; Define custom rules using JSON-LD format
(def uncle-rule
  {:id :ex/uncleRule
   :f/rule {:context default-context
            :where {:id '?person
                    :ex/parents {:ex/brother {:id '?uncle}}}
            :insert {:id '?person
                     :ex/uncle '?uncle}}})

;; Recursive rule example
(def ancestor-rule
  {:id :ex/ancestorRule
   :f/rule {:context default-context
            :where {:id '?person
                    :ex/parents {:id '?parent}}
            :insert {:id '?person
                     :ex/ancestor {:id '?parent}}}})

(def ancestor-transitive
  {:id :ex/ancestorTransitive
   :f/rule {:context default-context
            :where {:id '?person
                    :ex/ancestor {:ex/ancestor '?grandAncestor}}
            :insert {:id '?person
                     :ex/ancestor {:id '?grandAncestor}}}})

;; Insert rules into the database
@(fluree/insert! ledger (fluree/db ledger)
                 [uncle-rule ancestor-rule ancestor-transitive]
                 {:context (merge default-context
                                  {:f "https://ns.flur.ee/ledger#"})})

;; Apply datalog reasoning
(def reasoned-with-rules @(fluree/reason (fluree/db ledger) :datalog))

;; Or provide rules at reasoning time
(def reasoned-inline @(fluree/reason (fluree/db ledger) 
                                     :datalog 
                                     [uncle-rule ancestor-rule]))

;; Query inferred relationships
(def ancestors
  @(fluree/query reasoned-with-rules
                 {:context default-context
                  :select '[?person ?ancestor]
                  :where '{:id ?person
                           :ex/ancestor ?ancestor}}))
```

## Policies and Access Control

### Define Access Policies

```clojure
;; Role-based access policy
(def access-policy
  [{:id :ex/EmployeeViewPolicy
    :type :f/AccessPolicy
    :f/policyClass :ex/Employee
    :f/action [:f/view]
    :f/targetClass :ex/EmployeeData
    :f/where {:id '?data
              :ex/department '?dept
              :filter '(= ?dept ?userDepartment)}}])

;; Insert policy
@(fluree/insert! ledger (fluree/db ledger)
                 access-policy
                 {:context (merge default-context
                                  {:f "https://ns.flur.ee/ledger#"})})

;; Apply policy with identity context
(def alice-view
  @(fluree/query (fluree/db ledger)
                 {:context default-context
                  :select '[?data ?info]
                  :where '{:id ?data
                           :type :ex/EmployeeData
                           :ex/info ?info}
                  :opts {:identity :ex/alice}}))
```

### Property-Level Policies

```clojure
;; Restrict access to sensitive fields
(def property-policy
  [{:id :ex/SalaryRestriction
    :type :f/AccessPolicy
    :f/policyClass :ex/HRManager
    :f/action [:f/view]
    :f/targetProperty :ex/salary}])

;; Only HR managers can see salary data
@(fluree/insert! ledger (fluree/db ledger)
                 property-policy
                 {:context (merge default-context
                                  {:f "https://ns.flur.ee/ledger#"})})
```

## Advanced Features

### Multi-Format Support

```clojure
;; Same query in SPARQL
(def sparql-results
  @(fluree/query (fluree/db ledger)
                 "PREFIX schema: <http://schema.org/>
                  PREFIX ex: <http://example.org/>
                  
                  SELECT ?person ?name
                  WHERE {
                    ?person a schema:Person ;
                            schema:name ?name ;
                            ex:role ex:developer .
                  }"
                 {:format :sparql}))

;; Insert data as Turtle
@(fluree/insert! ledger (fluree/db ledger)
                 "@prefix ex: <http://example.org/> .
                  @prefix schema: <http://schema.org/> .
                  
                  ex:project2 a ex:Project ;
                              schema:name \"AI Platform\" ;
                              ex:lead ex:charlie ."
                 {:format :turtle})
```

### Verifiable Credentials

```clojure
;; Create signed credential
(def credential
  {:context ["https://www.w3.org/2018/credentials/v1" default-context]
   :type [:VerifiableCredential]
   :issuer :ex/company
   :credentialSubject {:id :ex/alice
                       :ex/role :ex/lead
                       :ex/clearance :ex/top-secret}})

;; Sign and submit credential (example - actual signing requires crypto libraries)
;; (def signed-cred (sign-credential credential private-key))
;; @(fluree/credential-update! conn signed-cred {})
```

### Cross-Ledger Queries

```clojure
;; Query across multiple ledgers
(def federated-results
  @(fluree/query (fluree/db ledger)
                 {:context default-context
                  :select '[?person ?project ?role]
                  :from ["employees" "projects"]
                  :where [{:id '?person
                           :schema/name '?name}
                          {:graph "projects"
                           :where {:id '?project
                                   :ex/member {:id '?person
                                               :ex/role '?role}}}]}))
```

### Sophisticated Transactional Updates

```clojure
;; Pattern-based updates - find and update matching data
@(fluree/update! conn 
                 {:ledger "my-app"
                  :where [{:id '?person
                           :schema/name "Bill"
                           :ex/department '?dept}]
                  :delete [{:id '?person
                            :schema/name "Bill"}]
                  :insert [{:id '?person
                            :schema/name "William"}]}
                 {:context default-context})

;; Complex business logic - promote employees based on conditions
@(fluree/update! conn
                 {:ledger "my-app"
                  :where [{:id '?person
                           :type :schema/Person
                           :ex/role :ex/developer
                           :ex/yearsExperience '?years}
                          {:filter '(>= ?years 5)}]
                  :delete [{:id '?person
                            :ex/role :ex/developer}]
                  :insert [{:id '?person
                            :ex/role :ex/senior-developer
                            :ex/promotedDate "2024-01-15"}]}
                 {:context default-context})
```

### Transaction Metadata

```clojure
;; Add metadata to commits
@(fluree/update! conn {:ledger "my-app"
                       :insert [{:id :ex/project1
                                 :ex/milestone "v2.0 release"}]}
                 {:context default-context
                  :message "Updated project milestone for Q4 release"
                  :author :ex/alice
                  :tag "v2.0-prep"})

;; Query commit metadata
(def commit-info
  @(fluree/query (fluree/db ledger)
                 {:context (merge default-context
                                  {:f "https://ns.flur.ee/ledger#"})
                  :select '[?commit ?message ?author ?time]
                  :where '{:id ?commit
                           :type :f/Commit
                           :f/message ?message
                           :f/author ?author
                           :f/time ?time}
                  :order-by '(desc ?time)
                  :limit 10}))
```

## Best Practices

### 1. Context Management
```clojure
;; Define contexts at namespace level
(def my-contexts
  {:default default-context
   :shacl (merge default-context
                 {:sh "http://www.w3.org/ns/shacl#"})
   :owl (merge default-context
               {:owl "http://www.w3.org/2002/07/owl#"})})

;; Use consistently across operations
(defn insert-with-context [ledger data context-key]
  @(fluree/insert! ledger (fluree/db ledger)
                   data
                   {:context (get my-contexts context-key)}))
```

### 2. Transaction Patterns
```clojure
;; Batch operations for efficiency
(defn batch-insert [ledger entities]
  @(fluree/insert! ledger (fluree/db ledger)
                   entities
                   {:context default-context}))

;; Use staging for complex updates
(defn complex-update [ledger new-data updates deletions]
  (let [db (fluree/db ledger)
        db1 @(fluree/insert db new-data {:context default-context})
        db2 @(fluree/update db1 updates {:context default-context})
        db3 @(fluree/delete db2 deletions {:context default-context})]
    @(fluree/commit! ledger db3 {:message "Complex multi-step update"})))
```

### 3. Query Optimization
```clojure
;; Use specific type constraints early
(def optimized-query
  {:context default-context
   :select '[?person ?name]
   :where [{:id '?person
            :type :schema/Person}  ; Type constraint first
           {:id '?person
            :schema/name '?name
            :ex/department :ex/engineering}]})

;; Limit large result sets
(defn paginated-query [offset]
  @(fluree/query (fluree/db ledger)
                 {:context default-context
                  :select '[?person ?name]
                  :where '{:id ?person
                           :type :schema/Person
                           :schema/name ?name}
                  :order-by '?name
                  :offset offset
                  :limit 100}))
```

## Comparison with Datomic/XTDB

### For Datomic Users
- Replace datoms `[e a v t]` with RDF triples via JSON-LD
- Datalog queries → Fluree analytical queries with similar patterns
- `:db/id` → `:id` with full IRI support
- Pull API → Graph crawl syntax in select
- Transaction functions → SHACL validation + smart functions

### For XTDB Users
- Documents → JSON-LD entities with `:id`
- Bitemporal queries → Time travel with commit history
- Datalog → Fluree analytical queries or SPARQL
- Schemaless → Optional SHACL validation
- Content addressing → Cryptographic commit proofs

## Resources

- [Fluree Documentation](https://docs.fluree.com)
- [JSON-LD Specification](https://www.w3.org/TR/json-ld11/)
- [SHACL Specification](https://www.w3.org/TR/shacl/)
- [OWL 2 Primer](https://www.w3.org/TR/owl2-primer/)

## Conclusion

With its combination of familiar Clojure idioms and semantic web standards,
Fluree provides a unique database solution that keeps your data verifiable,
queryable across time, and policy-protected. Whether you're building knowledge
graphs, linked data applications, or traditional database-backed applications,
Fluree's semantic web integration opens up new possibilities for data
interoperability and intelligent querying.