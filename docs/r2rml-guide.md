# R2RML Virtual Graph Guide for Fluree DB

## Overview

R2RML (RDB to RDF Mapping Language) is a W3C standard for expressing mappings from relational databases to RDF datasets. Fluree DB's R2RML support allows you to virtualize relational databases as RDF graphs, enabling SPARQL queries over SQL databases without data migration.

## Feature Support Status

### ✅ Supported Features

- **Logical Tables**
  - `rr:tableName` - Map database tables
  - `rr:sqlQuery` - Map SQL query results (including JOINs, aggregations, computed columns)

- **Subject Maps**
  - `rr:template` - Generate subject IRIs from column values
  - `rr:class` - Specify RDF classes for subjects

- **Predicate-Object Maps**
  - `rr:predicate` - Define predicates (as constants or IRIs)
  - `rr:column` - Map column values to objects
  - `rr:template` - Generate composite object values from multiple columns
  - `rr:constant` - Use fixed literal or IRI values
  - `rr:datatype` - Specify XSD datatypes for literals
  - `rr:language` - Add language tags to string literals

- **Format Support**
  - Turtle (.ttl) format for R2RML mappings
  - JSON-LD format for R2RML mappings
  - Inline mapping definitions

### ❌ Not Yet Supported

- `rr:termType` - Explicit term type specification (IRI, BlankNode, Literal)
- `rr:column` in subject maps - Column-based subject IRIs
- `rr:parentTriplesMap` - Foreign key relationships between mappings
- `rr:joinCondition` - Join conditions for related mappings
- `rr:graphMap` / `rr:graph` - Named graphs
- `rr:predicateMap` with columns/templates - Dynamic predicates
- `rr:sqlVersion` - SQL dialect specification
- `rr:inverseExpression` - Inverse property mappings
- `rr:defaultGraph` - Default graph specification

## Quick Start

### Setting Up a Virtual Graph

```clojure
(require '[fluree.db.nameservice.core :as nameservice])

;; Define R2RML mapping
(def r2rml-mapping
  "@prefix rr: <http://www.w3.org/ns/r2rml#> .
   @prefix ex: <http://example.com/> .
   @prefix foaf: <http://xmlns.com/foaf/0.1/> .
   
   ex:PersonMap a rr:TriplesMap ;
     rr:logicalTable [ rr:tableName \"persons\" ] ;
     rr:subjectMap [
       rr:template \"http://example.com/person/{id}\" ;
       rr:class foaf:Person
     ] ;
     rr:predicateObjectMap [
       rr:predicate foaf:name ;
       rr:objectMap [ rr:column \"name\" ]
     ] .")

;; Publish the virtual graph
(nameservice/publish publisher
  {:vg-name "vg/persons"
   :vg-type "fidx:R2RML"
   :engine  :r2rml
   :config  {:mappingInline r2rml-mapping
             :rdb {:jdbcUrl "jdbc:postgresql://localhost:5432/mydb"
                   :driver  "org.postgresql.Driver"
                   :user    "dbuser"
                   :password "dbpass"}}})
```

### Querying the Virtual Graph

#### FQL (JSON-LD Pattern)

```clojure
;; Query using Fluree's FQL with direct pattern matching (no 'graph' wrapper)
(def query
  {"from" ["vg/persons"]
   "select" ["?person" "?name"]
   "where" [{"@id" "?person"
            "@type" "http://xmlns.com/foaf/0.1/Person"
            "http://xmlns.com/foaf/0.1/name" "?name"}]})

(fluree/query-connection conn query)
;; Returns: [["http://example.com/person/1" "John Doe"]
;;           ["http://example.com/person/2" "Jane Smith"]]
```

#### FQL (With Graph Syntax)

```clojure
;; Alternative using explicit graph syntax (useful for federated queries)
(def query
  {"from" ["vg/persons"]
   "select" ["?person" "?name"]
   "where" [["graph" "vg/persons" 
            {"@id" "?person"
             "@type" "http://xmlns.com/foaf/0.1/Person"
             "http://xmlns.com/foaf/0.1/name" "?name"}]]})
```

#### SPARQL Equivalent

```sparql
PREFIX foaf: <http://xmlns.com/foaf/0.1/>

SELECT ?person ?name
WHERE {
  ?person a foaf:Person ;
          foaf:name ?name .
}
```

## Common R2RML Patterns

### 1. Basic Table Mapping

Map a simple database table to RDF triples:

```turtle
@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix ex: <http://example.com/> .

ex:EmployeeMap a rr:TriplesMap ;
  rr:logicalTable [ rr:tableName "employees" ] ;
  rr:subjectMap [
    rr:template "http://example.com/employee/{emp_id}" ;
    rr:class ex:Employee
  ] ;
  rr:predicateObjectMap [
    rr:predicate ex:firstName ;
    rr:objectMap [ rr:column "first_name" ]
  ] ;
  rr:predicateObjectMap [
    rr:predicate ex:lastName ;
    rr:objectMap [ rr:column "last_name" ]
  ] ;
  rr:predicateObjectMap [
    rr:predicate ex:salary ;
    rr:objectMap [ 
      rr:column "salary" ;
      rr:datatype xsd:decimal
    ]
  ] .
```

### 2. SQL Query as Logical Table

Use complex SQL queries with JOINs and aggregations:

```turtle
@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix ex: <http://example.com/> .

ex:CustomerOrderSummary a rr:TriplesMap ;
  rr:logicalTable [
    rr:sqlQuery "
      SELECT c.customer_id, c.name, COUNT(o.order_id) AS order_count,
             SUM(o.total) AS total_spent
      FROM customers c
      LEFT JOIN orders o ON c.customer_id = o.customer_id
      GROUP BY c.customer_id, c.name
    "
  ] ;
  rr:subjectMap [
    rr:template "http://example.com/customer/{customer_id}" ;
    rr:class ex:Customer
  ] ;
  rr:predicateObjectMap [
    rr:predicate ex:name ;
    rr:objectMap [ rr:column "name" ]
  ] ;
  rr:predicateObjectMap [
    rr:predicate ex:orderCount ;
    rr:objectMap [ 
      rr:column "order_count" ;
      rr:datatype xsd:integer
    ]
  ] ;
  rr:predicateObjectMap [
    rr:predicate ex:totalSpent ;
    rr:objectMap [ 
      rr:column "total_spent" ;
      rr:datatype xsd:decimal
    ]
  ] .
```

### 3. Template-Based Object Values

Create composite values from multiple columns:

```turtle
@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix ex: <http://example.com/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .

ex:PersonMap a rr:TriplesMap ;
  rr:logicalTable [ rr:tableName "persons" ] ;
  rr:subjectMap [
    rr:template "http://example.com/person/{person_id}" ;
    rr:class foaf:Person
  ] ;
  rr:predicateObjectMap [
    rr:predicate foaf:name ;
    rr:objectMap [ 
      rr:template "{first_name} {last_name}" 
    ]
  ] ;
  rr:predicateObjectMap [
    rr:predicate ex:identifier ;
    rr:objectMap [ 
      rr:template "PERSON-{person_id}-{department_code}" 
    ]
  ] .
```

### 4. Constant Values

Add static metadata to all mapped entities:

```turtle
@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix ex: <http://example.com/> .
@prefix dc: <http://purl.org/dc/terms/> .

ex:ProductMap a rr:TriplesMap ;
  rr:logicalTable [ rr:tableName "products" ] ;
  rr:subjectMap [
    rr:template "http://example.com/product/{product_id}" ;
    rr:class ex:Product
  ] ;
  rr:predicateObjectMap [
    rr:predicate ex:name ;
    rr:objectMap [ rr:column "product_name" ]
  ] ;
  rr:predicateObjectMap [
    rr:predicate dc:source ;
    rr:objectMap [ rr:constant "Legacy Database Import" ]
  ] ;
  rr:predicateObjectMap [
    rr:predicate ex:status ;
    rr:objectMap [ rr:constant ex:Active ]
  ] .
```

### 5. Language-Tagged Literals

Support internationalization with language tags:

```turtle
@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix ex: <http://example.com/> .
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .

ex:InternationalProductMap a rr:TriplesMap ;
  rr:logicalTable [ rr:tableName "products" ] ;
  rr:subjectMap [
    rr:template "http://example.com/product/{id}" ;
    rr:class ex:Product
  ] ;
  rr:predicateObjectMap [
    rr:predicate rdfs:label ;
    rr:objectMap [ 
      rr:column "name_en" ;
      rr:language "en"
    ]
  ] ;
  rr:predicateObjectMap [
    rr:predicate rdfs:label ;
    rr:objectMap [ 
      rr:column "name_es" ;
      rr:language "es"
    ]
  ] ;
  rr:predicateObjectMap [
    rr:predicate ex:description ;
    rr:objectMap [ 
      rr:template "{category} - {name_en}" ;
      rr:language "en-US"
    ]
  ] .
```

### 6. Data Types

Specify XSD data types for proper value interpretation:

```turtle
@prefix rr: <http://www.w3.org/ns/r2rml#> .
@prefix ex: <http://example.com/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .

ex:TransactionMap a rr:TriplesMap ;
  rr:logicalTable [ rr:tableName "transactions" ] ;
  rr:subjectMap [
    rr:template "http://example.com/transaction/{trans_id}" ;
    rr:class ex:Transaction
  ] ;
  rr:predicateObjectMap [
    rr:predicate ex:amount ;
    rr:objectMap [ 
      rr:column "amount" ;
      rr:datatype xsd:decimal
    ]
  ] ;
  rr:predicateObjectMap [
    rr:predicate ex:transactionDate ;
    rr:objectMap [ 
      rr:column "trans_date" ;
      rr:datatype xsd:dateTime
    ]
  ] ;
  rr:predicateObjectMap [
    rr:predicate ex:isProcessed ;
    rr:objectMap [ 
      rr:column "processed" ;
      rr:datatype xsd:boolean
    ]
  ] .
```

## JSON-LD Format

R2RML mappings can also be defined in JSON-LD format:

```json
{
  "@context": {
    "rr": "http://www.w3.org/ns/r2rml#",
    "ex": "http://example.com/",
    "foaf": "http://xmlns.com/foaf/0.1/"
  },
  "@id": "ex:PersonMap",
  "@type": "rr:TriplesMap",
  "rr:logicalTable": {
    "rr:tableName": "persons"
  },
  "rr:subjectMap": {
    "rr:template": "http://example.com/person/{id}",
    "rr:class": "foaf:Person"
  },
  "rr:predicateObjectMap": [
    {
      "rr:predicate": "foaf:firstName",
      "rr:objectMap": {
        "rr:column": "first_name"
      }
    },
    {
      "rr:predicate": "foaf:lastName",
      "rr:objectMap": {
        "rr:column": "last_name"
      }
    }
  ]
}
```

## Database Configuration

### PostgreSQL

```clojure
{:rdb {:jdbcUrl "jdbc:postgresql://localhost:5432/mydb"
       :driver  "org.postgresql.Driver"
       :user    "username"
       :password "password"}}
```

### MySQL

```clojure
{:rdb {:jdbcUrl "jdbc:mysql://localhost:3306/mydb"
       :driver  "com.mysql.cj.jdbc.Driver"
       :user    "username"
       :password "password"}}
```

### H2 (In-Memory)

```clojure
{:rdb {:jdbcUrl "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1"
       :driver  "org.h2.Driver"}}
```

### SQL Server

```clojure
{:rdb {:jdbcUrl "jdbc:sqlserver://localhost:1433;databaseName=mydb"
       :driver  "com.microsoft.sqlserver.jdbc.SQLServerDriver"
       :user    "username"
       :password "password"}}
```

## Query Examples

### Basic Pattern Matching

#### FQL (Direct Pattern)
```clojure
;; Find all persons and their names
{"from" ["vg/persons"]
 "select" ["?person" "?name"]
 "where" [{"@id" "?person"
          "@type" "http://xmlns.com/foaf/0.1/Person"
          "http://xmlns.com/foaf/0.1/name" "?name"}]}
```

#### SPARQL
```sparql
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
SELECT ?person ?name
WHERE {
  ?person a foaf:Person ;
          foaf:name ?name .
}
```

### Filtering with Literals

#### FQL
```clojure
;; Find persons with a specific name
{"from" ["vg/persons"]
 "select" ["?person"]
 "where" [{"@id" "?person"
          "@type" "http://xmlns.com/foaf/0.1/Person"
          "http://xmlns.com/foaf/0.1/name" "John Doe"}]}
```

#### SPARQL
```sparql
PREFIX foaf: <http://xmlns.com/foaf/0.1/>
SELECT ?person
WHERE {
  ?person a foaf:Person ;
          foaf:name "John Doe" .
}
```

### Multiple Predicates

#### FQL
```clojure
;; Find employees with salary information
{"from" ["vg/employees"]
 "select" ["?emp" "?firstName" "?lastName" "?salary"]
 "where" [{"@id" "?emp"
          "@type" "http://example.com/Employee"
          "http://example.com/firstName" "?firstName"
          "http://example.com/lastName" "?lastName"
          "http://example.com/salary" "?salary"}]}
```

#### SPARQL
```sparql
PREFIX ex: <http://example.com/>
SELECT ?emp ?firstName ?lastName ?salary
WHERE {
  ?emp a ex:Employee ;
       ex:firstName ?firstName ;
       ex:lastName ?lastName ;
       ex:salary ?salary .
}
```

### With Filter Expressions

#### FQL
```clojure
;; Find high-earning employees
{"from" ["vg/employees"]
 "select" ["?emp" "?name" "?salary"]
 "where" [{"@id" "?emp"
          "@type" "http://example.com/Employee"
          "http://example.com/name" "?name"
          "http://example.com/salary" "?salary"}]
 "filter" ["(> ?salary 100000)"]}
```

#### SPARQL
```sparql
PREFIX ex: <http://example.com/>
SELECT ?emp ?name ?salary
WHERE {
  ?emp a ex:Employee ;
       ex:name ?name ;
       ex:salary ?salary .
  FILTER (?salary > 100000)
}
```

## Best Practices

1. **Use Templates for Stable IRIs**: Always use column-based templates for subject IRIs to ensure consistent identifiers across queries.

2. **Specify Data Types**: Always specify appropriate XSD data types for numeric, date, and boolean values to ensure proper query behavior.

3. **Optimize SQL Queries**: When using `rr:sqlQuery`, ensure the SQL is optimized with appropriate indexes and limits.

4. **Use Constants for Metadata**: Use `rr:constant` for adding provenance or classification metadata that applies to all records.

5. **Language Tags for I18n**: Use `rr:language` for user-facing text fields to support internationalization.

6. **Template Composition**: Use templates to create meaningful composite values rather than concatenating in SQL.

7. **Column Naming**: Use consistent column naming conventions that map well to RDF predicates.

## Troubleshooting

### Common Issues

1. **Case Sensitivity**: H2 database returns column names in uppercase. The R2RML implementation handles this automatically by checking both cases.

2. **SQL Syntax**: Ensure SQL queries in `rr:sqlQuery` are compatible with your database dialect.

3. **Template Placeholders**: Column names in templates must match exactly (case-sensitive) with the column names in the result set.

4. **Data Type Mismatches**: Ensure XSD data types match the actual data format in the database.

5. **JDBC Driver**: Ensure the appropriate JDBC driver is available on the classpath.

## Performance Considerations

- **Query Pushdown**: The R2RML implementation generates SQL queries based on SPARQL patterns, pushing filters down to the database when possible.

- **Column Selection**: Only columns needed for the query are selected from the database.

- **Template Processing**: Templates are processed efficiently during result materialization.

- **Connection Pooling**: Consider using a connection pool for production deployments with high query volumes.

## Limitations

- Parent triples maps and join conditions are not yet supported, so foreign key relationships must be handled via SQL JOINs in `rr:sqlQuery`.

- Named graphs are not supported; all triples are in the default graph.

- Dynamic predicates (from columns) are not supported; predicates must be constants.

- Blank nodes are not supported; all subjects must be IRIs.

## References

- [W3C R2RML Specification](https://www.w3.org/TR/r2rml/)
- [Fluree DB Documentation](https://developers.flur.ee/)
- [RDF 1.1 Concepts](https://www.w3.org/TR/rdf11-concepts/)
- [SPARQL 1.1 Query Language](https://www.w3.org/TR/sparql11-query/)