(ns fluree.db.virtual-graph.r2rml-test
  "Integration tests for R2RML virtual graph functionality.
  Tests the mapping of relational data to RDF through R2RML mappings."
  (:require [clojure.core.async :as async]
            [clojure.java.jdbc :as jdbc]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [fluree.db.api :as fluree]
            [fluree.db.connection.config :as config]
            [fluree.db.connection.system :as system]
            [fluree.db.nameservice :as nameservice]))

;; Test database schema and data
(def h2-spec
  {:classname "org.h2.Driver"
   :subprotocol "h2"
   :subname "mem:testdb;DB_CLOSE_DELAY=-1"})

(def create-sql
  ["CREATE TABLE people (id INTEGER PRIMARY KEY, name VARCHAR(255))"
   "CREATE TABLE customers (customer_id INTEGER PRIMARY KEY, first_name VARCHAR(255), last_name VARCHAR(255), email VARCHAR(255), created_at TIMESTAMP)"
   "CREATE TABLE orders (order_id INTEGER PRIMARY KEY, customer_id INTEGER, order_date TIMESTAMP, status VARCHAR(50), total_amount DECIMAL(10,2))"
   "CREATE TABLE products (product_id INTEGER PRIMARY KEY, sku VARCHAR(50), name VARCHAR(255), description TEXT, price DECIMAL(10,2), stock_quantity INTEGER)"
   "CREATE TABLE order_items (order_item_id INTEGER PRIMARY KEY, order_id INTEGER, product_id INTEGER, quantity INTEGER, unit_price DECIMAL(10,2))"

   ;; Insert test data
   "INSERT INTO people VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')"

   "INSERT INTO customers VALUES 
    (1, 'John', 'Doe', 'john@example.com', '2023-01-01 10:00:00'),
    (2, 'Jane', 'Smith', 'jane@example.com', '2023-01-02 11:00:00'),
    (3, 'Bob', 'Johnson', 'bob@example.com', '2023-01-03 12:00:00'),
    (4, 'Alice', 'Brown', 'alice@example.com', '2023-01-04 13:00:00')"

   "INSERT INTO products VALUES 
    (1, 'SKU001', 'Laptop', 'High-performance laptop', 999.99, 10),
    (2, 'SKU002', 'Mouse', 'Wireless mouse', 29.99, 50),
    (3, 'SKU003', 'Keyboard', 'Mechanical keyboard', 89.99, 25),
    (4, 'SKU004', 'Monitor', '4K monitor', 299.99, 15),
    (5, 'SKU005', 'Headphones', 'Noise-cancelling headphones', 199.99, 30)"

   "INSERT INTO orders VALUES 
    (1, 1, '2023-02-01 09:00:00', 'completed', 1029.98),
    (2, 2, '2023-02-02 10:00:00', 'completed', 89.99),
    (3, 1, '2023-02-03 11:00:00', 'pending', 299.99),
    (4, 3, '2023-02-04 12:00:00', 'completed', 199.99),
    (5, 4, '2023-02-05 13:00:00', 'cancelled', 999.99)"

   "INSERT INTO order_items VALUES 
    (1, 1, 1, 1, 999.99),
    (2, 1, 2, 1, 29.99),
    (3, 2, 3, 1, 89.99),
    (4, 3, 4, 1, 299.99),
    (5, 4, 5, 1, 199.99),
    (6, 5, 1, 1, 999.99)"])

;; R2RML mapping definition
(def r2rml-ttl (str "@prefix rr: <http://www.w3.org/ns/r2rml#> .\n"
                    "@prefix ex: <http://example.com/> .\n"
                    "@prefix foaf: <http://xmlns.com/foaf/0.1/> .\n"
                    "@prefix dcterms: <http://purl.org/dc/terms/> .\n"
                    "@prefix schema: <http://schema.org/> .\n"
                    "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n"
                    "@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#> .\n\n"

                    "ex:PeopleMap a rr:TriplesMap ;\n"
                    "    rr:logicalTable [ rr:tableName \"people\" ] ;\n"
                    "    rr:subjectMap [\n"
                    "        rr:template \"http://example.com/person/{id}\" ;\n"
                    "        rr:class ex:Person ;\n"
                    "    ] ;\n"
                    "    rr:predicateObjectMap [\n"
                    "        rr:predicate schema:name ;\n"
                    "        rr:objectMap [ rr:column \"name\" ]\n"
                    "    ] .\n\n"

                    "ex:CustomersMap a rr:TriplesMap ;\n"
                    "    rr:logicalTable [ rr:tableName \"customers\" ] ;\n"
                    "    rr:subjectMap [\n"
                    "        rr:template \"http://example.com/customer/{customer_id}\" ;\n"
                    "        rr:class ex:Customer ;\n"
                    "    ] ;\n"
                    "    rr:predicateObjectMap [\n"
                    "        rr:predicate foaf:firstName ;\n"
                    "        rr:objectMap [ rr:column \"first_name\" ]\n"
                    "    ] ;\n"
                    "    rr:predicateObjectMap [\n"
                    "        rr:predicate foaf:lastName ;\n"
                    "        rr:objectMap [ rr:column \"last_name\" ]\n"
                    "    ] ;\n"
                    "    rr:predicateObjectMap [\n"
                    "        rr:predicate foaf:mbox ;\n"
                    "        rr:objectMap [ rr:column \"email\" ]\n"
                    "    ] ;\n"
                    "    rr:predicateObjectMap [\n"
                    "        rr:predicate dcterms:created ;\n"
                    "        rr:objectMap [ rr:column \"created_at\" ]\n"
                    "    ] .\n\n"

                    "ex:OrdersMap a rr:TriplesMap ;\n"
                    "    rr:logicalTable [ rr:tableName \"orders\" ] ;\n"
                    "    rr:subjectMap [\n"
                    "        rr:template \"http://example.com/order/{order_id}\" ;\n"
                    "        rr:class ex:Order ;\n"
                    "    ] ;\n"
                    "    rr:predicateObjectMap [\n"
                    "        rr:predicate dcterms:date ;\n"
                    "        rr:objectMap [ rr:column \"order_date\" ]\n"
                    "    ] ;\n"
                    "    rr:predicateObjectMap [\n"
                    "        rr:predicate ex:totalAmount ;\n"
                    "        rr:objectMap [ rr:column \"total_amount\" ]\n"
                    "    ] ;\n"
                    "    rr:predicateObjectMap [\n"
                    "        rr:predicate ex:status ;\n"
                    "        rr:objectMap [ rr:column \"status\" ]\n"
                    "    ] .\n\n"

                    "ex:ProductsMap a rr:TriplesMap ;\n"
                    "    rr:logicalTable [ rr:tableName \"products\" ] ;\n"
                    "    rr:subjectMap [\n"
                    "        rr:template \"http://example.com/product/{product_id}\" ;\n"
                    "        rr:class ex:Product ;\n"
                    "    ] ;\n"
                    "    rr:predicateObjectMap [\n"
                    "        rr:predicate ex:sku ;\n"
                    "        rr:objectMap [ rr:column \"sku\" ]\n"
                    "    ] ;\n"
                    "    rr:predicateObjectMap [\n"
                    "        rr:predicate rdfs:label ;\n"
                    "        rr:objectMap [ rr:column \"name\" ]\n"
                    "    ] ;\n"
                    "    rr:predicateObjectMap [\n"
                    "        rr:predicate dcterms:description ;\n"
                    "        rr:objectMap [ rr:column \"description\" ]\n"
                    "    ] ;\n"
                    "    rr:predicateObjectMap [\n"
                    "        rr:predicate ex:price ;\n"
                    "        rr:objectMap [ rr:column \"price\" ]\n"
                    "    ] ;\n"
                    "    rr:predicateObjectMap [\n"
                    "        rr:predicate ex:stockQuantity ;\n"
                    "        rr:objectMap [ rr:column \"stock_quantity\" ]\n"
                    "    ] .\n\n"

                    "ex:OrderItemsMap a rr:TriplesMap ;\n"
                    "    rr:logicalTable [ rr:tableName \"order_items\" ] ;\n"
                    "    rr:subjectMap [\n"
                    "        rr:template \"http://example.com/order-item/{order_item_id}\" ;\n"
                    "        rr:class ex:OrderItem ;\n"
                    "    ] ;\n"
                    "    rr:predicateObjectMap [\n"
                    "        rr:predicate ex:quantity ;\n"
                    "        rr:objectMap [ rr:column \"quantity\" ]\n"
                    "    ] ;\n"
                    "    rr:predicateObjectMap [\n"
                    "        rr:predicate ex:unitPrice ;\n"
                    "        rr:objectMap [ rr:column \"unit_price\" ]\n"
                    "    ] ."))

;; Test fixtures
(def ^:private test-system (atom nil))
(def ^:private test-conn (atom nil))
(def ^:private test-publisher (atom nil))

(defn setup-h2-database
  "Initialize H2 database with test data"
  []
  (jdbc/with-db-connection [conn h2-spec]
    (doseq [s create-sql]
      (jdbc/execute! conn [s]))))

(defn setup-fluree-system
  "Set up Fluree system and publish R2RML virtual graph"
  []
  (let [memory-config {"@context" {"@base"  "https://ns.flur.ee/config/connection/"
                                   "@vocab" "https://ns.flur.ee/system#"}
                       "@id"      "memory"
                       "@graph"   [{"@id"   "memoryStorage"
                                    "@type" "Storage"}
                                   {"@id"              "connection"
                                    "@type"            "Connection"
                                    "parallelism"      4
                                    "cacheMaxMb"       1000
                                    "commitStorage"    {"@id" "memoryStorage"}
                                    "indexStorage"     {"@id" "memoryStorage"}
                                    "primaryPublisher" {"@type"   "Publisher"
                                                        "storage" {"@id" "memoryStorage"}}}]}
        sys (system/initialize (config/parse memory-config))]
    (reset! test-system sys)
    (reset! test-conn (some (fn [[k v]] (when (isa? k :fluree.db/connection) v)) sys))
    (reset! test-publisher (some (fn [[k v]] (when (isa? k :fluree.db.nameservice/storage) v)) sys))
    ;; Publish R2RML virtual graph
    (let [tmp-file (java.io.File/createTempFile "r2rml" ".ttl")]
      (spit tmp-file r2rml-ttl)
      (async/<!! (nameservice/publish @test-publisher {:vg-name "vg/sql"
                                                       :vg-type "fidx:R2RML"
                                                       :engine  :r2rml
                                                       :config  {:mapping (.getAbsolutePath tmp-file)
                                                                 :rdb {:jdbcUrl "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1"
                                                                       :driver  "org.h2.Driver"}}
                                                       :dependencies ["dummy-ledger@main"]})))))

(use-fixtures :once (fn [f]
                      (setup-h2-database)
                      (setup-fluree-system)
                      (f)))

;; Integration Tests

(deftest r2rml-basic-mapping-integration-test
  (testing "R2RML correctly maps relational data to RDF triples"
    (let [query {"from" ["vg/sql"]
                 "select" ["?s" "?name"]
                 "where" [["graph" "vg/sql" {"@id" "?s"
                                             "http://schema.org/name" "?name"}]]}
          res @(fluree/query-connection @test-conn query)
          expected #{["http://example.com/person/1" "Alice"]
                     ["http://example.com/person/2" "Bob"]
                     ["http://example.com/person/3" "Charlie"]}]
      (is (= expected (set res))
          "Should return exactly the 3 people with their correct IRIs and names"))))

(deftest r2rml-complex-mapping-integration-test
  (testing "R2RML handles multiple tables with different data types and vocabularies"
    ;; Test customer data mapping with FOAF vocabulary
    (let [query {"from" ["vg/sql"]
                 "select" ["?firstName" "?lastName" "?email"]
                 "where" [["graph" "vg/sql" {"@id" "?customer"
                                             "@type" "http://example.com/Customer"
                                             "http://xmlns.com/foaf/0.1/firstName" "?firstName"
                                             "http://xmlns.com/foaf/0.1/lastName" "?lastName"
                                             "http://xmlns.com/foaf/0.1/mbox" "?email"}]]}
          res @(fluree/query-connection @test-conn query)
          expected-set #{["John" "Doe" "john@example.com"]
                         ["Jane" "Smith" "jane@example.com"]
                         ["Bob" "Johnson" "bob@example.com"]
                         ["Alice" "Brown" "alice@example.com"]}]
      (is (= expected-set (set res)) "Should return exact customer data"))

    ;; Test order data mapping with decimal amounts
    (let [query {"from" ["vg/sql"]
                 "select" ["?order" "?totalAmount" "?status"]
                 "where" [["graph" "vg/sql" {"@id" "?order"
                                             "@type" "http://example.com/Order"
                                             "http://example.com/totalAmount" "?totalAmount"
                                             "http://example.com/status" "?status"}]]}
          res @(fluree/query-connection @test-conn query)
          expected #{["http://example.com/order/1" 1029.98M "completed"]
                     ["http://example.com/order/2" 89.99M "completed"]
                     ["http://example.com/order/3" 299.99M "pending"]
                     ["http://example.com/order/4" 199.99M "completed"]
                     ["http://example.com/order/5" 999.99M "cancelled"]}]
      (is (= expected (set res))
          "Should return all 5 orders with correct IRIs, amounts, and statuses"))))

(deftest r2rml-aggregate-query-integration-test
  (testing "R2RML supports Fluree aggregate functions in SELECT"
    ;; Test COUNT aggregate
    (let [count-query {"from" ["vg/sql"]
                       "select" ["(count ?order)"]
                       "where" [["graph" "vg/sql" {"@id" "?order"
                                                   "@type" "http://example.com/Order"}]]}
          res @(fluree/query-connection @test-conn count-query)]
      (is (= [[5]] res) "COUNT should return 5 orders"))))

(deftest r2rml-literal-value-filtering-test
  (testing "R2RML supports filtering by literal values in WHERE clauses"
    ;; This test demonstrates filtering orders by status="completed" 
    ;; where "completed" is a literal string value, not a variable binding
    (let [query {"from" ["vg/sql"]
                 "select" ["?order" "?amount"]
                 "where" [["graph" "vg/sql" {"@id" "?order"
                                             "@type" "http://example.com/Order"
                                             "http://example.com/totalAmount" "?amount"
                                             "http://example.com/status" "completed"}]]} ; <- "completed" is a literal filter
          res @(fluree/query-connection @test-conn query)
          order-ids (set (map first res))
          amounts (map second res)]
      ;; Orders 1, 2, and 4 have status "completed" in our test data
      (is (= 3 (count res)) "Should return only 3 completed orders")
      (is (= #{"http://example.com/order/1"
               "http://example.com/order/2"
               "http://example.com/order/4"}
             order-ids)
          "Should return specific completed order IDs")
      (is (= #{1029.98M 89.99M 199.99M} (set amounts))
          "Should return only completed order amounts")
      (is (= 1319.96M (reduce + 0M amounts))
          "Sum of completed orders should be 1319.96"))))

(deftest r2rml-simplified-syntax-test
  (testing "R2RML queries work without [:graph ...] wrapper syntax using BM25-style pattern collection"
    ;; Test that we can query directly with a map in the where clause
    ;; This should now work with the new -match-triple/-finalize approach
    (let [query {"from" ["vg/sql"]
                 "select" ["?order" "?amount"]
                 "where" {"@id" "?order"
                          "@type" "http://example.com/Order"
                          "http://example.com/totalAmount" "?amount"
                          "http://example.com/status" "completed"}}
          res @(fluree/query-connection @test-conn query)
          order-ids (set (map first res))
          amounts (map second res)]
      ;; Should get the same results as the test with [:graph ...] wrapper
      (is (= 3 (count res)) "Should return only 3 completed orders")
      (is (= #{"http://example.com/order/1"
               "http://example.com/order/2"
               "http://example.com/order/4"}
             order-ids)
          "Should return specific completed order IDs")
      (is (= #{1029.98M 89.99M 199.99M} (set amounts))
          "Should return only completed order amounts"))))

(deftest r2rml-context-iri-expansion-test
  (testing "R2RML correctly handles @context for IRI expansion and compaction"
    ;; Test with context that defines prefixes for our vocabulary
    (let [query {"from" ["vg/sql"]
                 "@context" {"@vocab" "http://example.com/"
                             "schema" "http://schema.org/"
                             "foaf" "http://xmlns.com/foaf/0.1/"
                             "dcterms" "http://purl.org/dc/terms/"}
                 "select" ["?customer" "?firstName" "?lastName" "?email"]
                 "where" {"@id" "?customer"
                          "@type" "Customer"  ;; Uses @vocab expansion
                          "foaf:firstName" "?firstName"  ;; Uses prefix expansion
                          "foaf:lastName" "?lastName"
                          "foaf:mbox" "?email"}}
          res @(fluree/query-connection @test-conn query)]
      (is (= 4 (count res)) "Should return all 4 customers")
      (is (= #{"John" "Jane" "Bob" "Alice"}
             (set (map second res)))
          "Should return all customer first names")
      (is (every? #(str/ends-with? (nth % 3) "@example.com") res)
          "All emails should end with @example.com")))

    ;; Test with different context for orders using @vocab
  (let [query {"from" ["vg/sql"]
               "@context" {"@vocab" "http://example.com/"
                           "@base" "http://example.com/"}
               "select" ["?order" "?amount"]
               "where" {"@id" "?order"
                        "@type" "Order"  ;; Expands to http://example.com/Order
                        "totalAmount" "?amount"  ;; Expands to http://example.com/totalAmount
                        "status" "completed"}}  ;; Expands to http://example.com/status
        res @(fluree/query-connection @test-conn query)]
    (is (= 3 (count res)) "Should return 3 completed orders")
    (is (= #{1029.98M 89.99M 199.99M} (set (map second res)))
        "Should return correct order amounts"))

    ;; Test mixed context with both prefix and vocab
  (let [query {"from" ["vg/sql"]
               "@context" {"@vocab" "http://default.org/"
                           "ex" "http://example.com/"
                           "schema" "http://schema.org/"}
               "select" ["?person" "?name"]
               "where" {"@id" "?person"
                        "@type" "ex:Person"  ;; Uses prefix
                        "schema:name" "?name"}}  ;; Uses prefix
        res @(fluree/query-connection @test-conn query)]
    (is (= 3 (count res)) "Should return all 3 people")
    (is (= #{"Alice" "Bob" "Charlie"} (set (map second res)))
        "Should return all person names")))

(deftest r2rml-context-with-graph-clause-test
  (testing "R2RML @context works with explicit [:graph ...] syntax too"
    (let [query {"from" ["vg/sql"]
                 "@context" {"ex" "http://example.com/"
                             "schema" "http://schema.org/"}
                 "select" ["?product" "?sku" "?price"]
                 "where" [["graph" "vg/sql"
                           {"@id" "?product"
                            "@type" "ex:Product"
                            "ex:sku" "?sku"
                            "ex:price" "?price"}]]}
          res @(fluree/query-connection @test-conn query)]
      (is (= 5 (count res)) "Should return all 5 products")
      (is (every? #(str/starts-with? (second %) "SKU") res)
          "All SKUs should start with 'SKU'")
      (is (= 1619.95M (reduce + (map #(nth % 2) res)))
          "Sum of all product prices should be 1619.95"))))

(deftest r2rml-iri-compaction-in-results-test
  (testing "R2RML properly returns IRIs in query results"
    ;; Test that subject IRIs are returned in results
    (let [query {"from" ["vg/sql"]
                 "@context" {"ex" "http://example.com/"
                             "schema" "http://schema.org/"}
                 "select" ["?person" "?name"]
                 "where" {"@id" "?person"
                          "@type" "ex:Person"
                          "schema:name" "?name"}}
          res @(fluree/query-connection @test-conn query)]
      (is (= 3 (count res)) "Should return all 3 people")
      ;; Check that IRIs are COMPACTED using the context prefix
      (is (every? #(str/starts-with? (first %) "ex:person/") res)
          "Person IRIs should be compacted with 'ex:' prefix")
      ;; Check that we get the names
      (is (= #{"Alice" "Bob" "Charlie"}
             (set (map second res)))
          "Should return all person names")
      ;; Verify specific compacted person IRIs
      (let [person-iris (set (map first res))]
        (is (contains? person-iris "ex:person/1") "Should have compacted ex:person/1")
        (is (contains? person-iris "ex:person/2") "Should have compacted ex:person/2")
        (is (contains? person-iris "ex:person/3") "Should have compacted ex:person/3")))

    ;; Test with correct FOAF prefix to verify compaction
    (let [query {"from" ["vg/sql"]
                 "@context" {"foaf" "http://xmlns.com/foaf/0.1/"}
                 "select" ["?customer" "?firstName"]
                 "where" {"@id" "?customer"
                          "@type" "http://example.com/Customer"  ;; Full IRI since no ex: prefix
                          "foaf:firstName" "?firstName"}}
          res @(fluree/query-connection @test-conn query)]
      (is (= 4 (count res)) "Should return all 4 customers")
      ;; Check that customer IRIs are NOT compacted (no prefix defined for them)
      (is (every? #(str/starts-with? (first %) "http://example.com/customer/") res)
          "Customer IRIs should be full IRIs without prefix compaction"))

    ;; Test with ORDER IRIs to see full IRI paths
    (let [query {"from" ["vg/sql"]
                 "@context" {"ex" "http://example.com/"}
                 "select" ["?order" "?status"]
                 "where" {"@id" "?order"
                          "@type" "ex:Order"
                          "ex:status" "?status"}}
          res @(fluree/query-connection @test-conn query)]
      (is (= 5 (count res)) "Should return all 5 orders")
      ;; Verify order IRIs are COMPACTED
      (is (every? #(str/starts-with? (first %) "ex:order/") res)
          "Order IRIs should be compacted with 'ex:' prefix")
      ;; Check status values
      (is (= #{"completed" "pending" "cancelled"}
             (set (map second res)))
          "Should have all three order statuses"))

    ;; Test with no context to see full IRIs
    (let [query {"from" ["vg/sql"]
                 "select" ["?product" "?sku"]
                 "where" [["graph" "vg/sql"
                           {"@id" "?product"
                            "@type" "http://example.com/Product"
                            "http://example.com/sku" "?sku"}]]}
          res @(fluree/query-connection @test-conn query)]
      (is (= 5 (count res)) "Should return all 5 products")
      ;; Full IRIs should be returned without context
      (is (every? #(re-matches #"^http://example\.com/product/\d+$" (first %)) res)
          "Product IRIs should be full IRIs matching the pattern"))))

(deftest r2rml-data-type-handling-test
  (testing "R2RML correctly handles various SQL data types"
    ;; Test integer columns
    (let [query {"from" ["vg/sql"]
                 "select" ["?stock"]
                 "where" [["graph" "vg/sql" {"@id" "?product"
                                             "@type" "http://example.com/Product"
                                             "http://example.com/stockQuantity" "?stock"}]]}
          res @(fluree/query-connection @test-conn query)
          stocks (map first res)
          expected-stocks #{10 50 25 15 30}]  ; From products table: (1,10), (2,50), (3,25), (4,15), (5,30)
      (is (= expected-stocks (set stocks))
          "Should return exact stock quantities: 10, 50, 25, 15, 30")
      (is (every? integer? stocks) "All stock values should be integers"))

    ;; Test decimal columns
    (let [query {"from" ["vg/sql"]
                 "select" ["?price"]
                 "where" [["graph" "vg/sql" {"@id" "?product"
                                             "@type" "http://example.com/Product"
                                             "http://example.com/price" "?price"}]]}
          res @(fluree/query-connection @test-conn query)
          prices (map first res)
          expected-prices #{999.99M 29.99M 89.99M 299.99M 199.99M}]  ; From products table
      (is (= expected-prices (set prices))
          "Should return exact prices: 999.99, 29.99, 89.99, 299.99, 199.99")
      (is (every? decimal? prices) "All prices should be decimals"))

    ;; Test timestamp columns
    (let [query {"from" ["vg/sql"]
                 "select" ["?created"]
                 "where" [["graph" "vg/sql" {"@id" "?customer"
                                             "@type" "http://example.com/Customer"
                                             "http://purl.org/dc/terms/created" "?created"}]]}
          res @(fluree/query-connection @test-conn query)]
      (is (= 4 (count res)) "Should have 4 customers with creation dates")
      (is (every? #(string? (first %)) res) "Timestamps should be strings"))))

(deftest r2rml-rdf-type-mapping-test
  (testing "R2RML class mappings generate correct rdf:type triples for specific types"
    ;; Note: Generic type queries across all mappings not yet supported
    ;; Test each type individually
    (testing "Order type"
      (let [query {"from" ["vg/sql"]
                   "select" ["?s"]
                   "where" [["graph" "vg/sql" {"@id" "?s"
                                               "@type" "http://example.com/Order"}]]}
            res @(fluree/query-connection @test-conn query)]
        (is (= 5 (count res)) "Should have 5 orders")))

    (testing "Product type"
      (let [query {"from" ["vg/sql"]
                   "select" ["?s"]
                   "where" [["graph" "vg/sql" {"@id" "?s"
                                               "@type" "http://example.com/Product"}]]}
            res @(fluree/query-connection @test-conn query)]
        (is (= 5 (count res)) "Should have 5 products")))

    (testing "Customer type"
      (let [query {"from" ["vg/sql"]
                   "select" ["?s"]
                   "where" [["graph" "vg/sql" {"@id" "?s"
                                               "@type" "http://example.com/Customer"}]]}
            res @(fluree/query-connection @test-conn query)]
        (is (= 4 (count res)) "Should have 4 customers")))

    (testing "Person type"
      (let [query {"from" ["vg/sql"]
                   "select" ["?s"]
                   "where" [["graph" "vg/sql" {"@id" "?s"
                                               "@type" "http://example.com/Person"}]]}
            res @(fluree/query-connection @test-conn query)]
        (is (= 3 (count res)) "Should have 3 people")))))

(deftest r2rml-template-uri-generation-test
  (testing "R2RML correctly generates URIs from templates"
    ;; Test URI generation for orders
    (let [query {"from" ["vg/sql"]
                 "select" ["?order"]
                 "where" [["graph" "vg/sql" {"@id" "?order"
                                             "@type" "http://example.com/Order"}]]}
          res @(fluree/query-connection @test-conn query)
          order-uris (map first res)]
      (is (every? #(re-matches #"^http://example.com/order/\d+$" %) order-uris)
          "All order URIs should match the template pattern"))

    ;; Test URI generation for products
    (let [query {"from" ["vg/sql"]
                 "select" ["?product"]
                 "where" [["graph" "vg/sql" {"@id" "?product"
                                             "@type" "http://example.com/Product"}]]}
          res @(fluree/query-connection @test-conn query)
          product-uris (map first res)]
      (is (every? #(re-matches #"^http://example.com/product/\d+$" %) product-uris)
          "All product URIs should match the template pattern"))))

(deftest r2rml-filter-test
  (testing "R2RML correctly handles filter expressions in WHERE clause"
    ;; Test basic filter with string comparison on name
    (let [query {"from" ["vg/sql"]
                 "@context" {"ex" "http://example.com/"
                             "schema" "http://schema.org/"}
                 "select" ["?person" "?name"]
                 "where" [["graph" "vg/sql"
                           {"@id" "?person"
                            "@type" "ex:Person"
                            "schema:name" "?name"}]
                          ["filter" "(= ?name \"Alice\")"]]}
          res @(fluree/query-connection @test-conn query)]
      (is (= [["ex:person/1" "Alice"]] res)
          "Should return only Alice with her compacted IRI"))

    ;; Test filter with string comparison
    (let [query {"from" ["vg/sql"]
                 "@context" {"ex" "http://example.com/"
                             "foaf" "http://xmlns.com/foaf/0.1/"}
                 "select" ["?customer" "?firstName"]
                 "where" [["graph" "vg/sql"
                           {"@id" "?customer"
                            "@type" "ex:Customer"
                            "foaf:firstName" "?firstName"}]
                          ["filter" "(= ?firstName \"John\")"]]}
          res @(fluree/query-connection @test-conn query)]
      (is (= [["ex:customer/1" "John"]] res)
          "Should return only John (customer 1) with compacted IRI"))

    ;; Test multiple filters
    (let [query {"from" ["vg/sql"]
                 "@context" {"ex" "http://example.com/"}
                 "select" ["?order" "?total"]
                 "where" [["graph" "vg/sql"
                           {"@id" "?order"
                            "@type" "ex:Order"
                            "ex:totalAmount" "?total"
                            "ex:status" "?status"}]
                          ["filter" "(> ?total 100.00)"]
                          ["filter" "(= ?status \"completed\")"]]}
          res @(fluree/query-connection @test-conn query)]
      (is (= #{["ex:order/1" 1029.98M]
               ["ex:order/4" 199.99M]}
             (set res))
          "Should return only completed orders over $100: order/1 (1029.98) and order/4 (199.99)"))

    ;; Test filter with string comparison on lastName
    (let [query {"from" ["vg/sql"]
                 "@context" {"ex" "http://example.com/"
                             "foaf" "http://xmlns.com/foaf/0.1/"}
                 "select" ["?customer" "?lastName"]
                 "where" [["graph" "vg/sql"
                           {"@id" "?customer"
                            "@type" "ex:Customer"
                            "foaf:lastName" "?lastName"}]
                          ["filter" "(= ?lastName \"Smith\")"]]}
          res @(fluree/query-connection @test-conn query)]
      (is (= [["ex:customer/2" "Smith"]] res)
          "Should return only Jane Smith (customer 2) with compacted IRI"))))

(deftest r2rml-inline-mapping-test
  (testing "R2RML supports inline TTL mappings instead of file-based"
    ;; Create an inline R2RML mapping as a string
    (let [inline-ttl (str "@prefix rr: <http://www.w3.org/ns/r2rml#> .\n"
                          "@prefix ex: <http://example.com/> .\n"
                          "@prefix schema: <http://schema.org/> .\n\n"
                          "ex:SimplePeopleMap a rr:TriplesMap ;\n"
                          "    rr:logicalTable [ rr:tableName \"people\" ] ;\n"
                          "    rr:subjectMap [\n"
                          "        rr:template \"http://example.com/person/{id}\" ;\n"
                          "        rr:class ex:Person ;\n"
                          "    ] ;\n"
                          "    rr:predicateObjectMap [\n"
                          "        rr:predicate schema:name ;\n"
                          "        rr:objectMap [ rr:column \"name\" ]\n"
                          "    ] .\n")
          ;; Create a test system with inline mapping
          test-system-inline (system/initialize
                              (config/parse
                               {"@context" {"@base"  "https://ns.flur.ee/config/connection/"
                                            "@vocab" "https://ns.flur.ee/system#"}
                                "@id"      "memory"
                                "@graph"   [{"@id"   "memoryStorage"
                                             "@type" "Storage"}
                                            {"@id"              "connection"
                                             "@type"            "Connection"
                                             "parallelism"      4
                                             "cacheMaxMb"       1000
                                             "commitStorage"    {"@id" "memoryStorage"}
                                             "indexStorage"     {"@id" "memoryStorage"}
                                             "primaryPublisher" {"@type"   "Publisher"
                                                                 "storage" {"@id" "memoryStorage"}}}]}))
          conn-inline (some (fn [[k v]] (when (isa? k :fluree.db/connection) v)) test-system-inline)
          publisher-inline (some (fn [[k v]] (when (isa? k :fluree.db.nameservice/storage) v)) test-system-inline)]
      ;; Publish R2RML with inline mapping
      (async/<!! (nameservice/publish publisher-inline {:vg-name "vg/inline-sql"
                                                        :vg-type "fidx:R2RML"
                                                        :engine  :r2rml
                                                        :config  {:mappingInline inline-ttl
                                                                  :rdb {:jdbcUrl "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1"
                                                                        :driver  "org.h2.Driver"}}
                                                        :dependencies ["dummy-ledger@main"]}))
      ;; Query using the inline mapping
      (let [query {"from" ["vg/inline-sql"]
                   "select" ["?s" "?name"]
                   "where" [["graph" "vg/inline-sql" {"@id" "?s"
                                                      "http://schema.org/name" "?name"}]]}
            res @(fluree/query-connection conn-inline query)
            expected #{["http://example.com/person/1" "Alice"]
                       ["http://example.com/person/2" "Bob"]
                       ["http://example.com/person/3" "Charlie"]}]
        (is (= expected (set res))
            "Should return correct data using inline R2RML mapping")))))

(deftest r2rml-json-ld-mapping-test
  (testing "R2RML supports JSON-LD format mappings"
    ;; Create an R2RML mapping in JSON-LD format
    (let [json-ld-mapping {"@context" {"rr" "http://www.w3.org/ns/r2rml#"
                                       "ex" "http://example.com/"
                                       "schema" "http://schema.org/"}
                           "@id" "ex:CustomersJSONMap"
                           "@type" "rr:TriplesMap"
                           "rr:logicalTable" {"rr:tableName" "customers"}
                           "rr:subjectMap" {"rr:template" "http://example.com/customer/{customer_id}"
                                            "rr:class" "ex:Customer"}
                           "rr:predicateObjectMap" [{"rr:predicate" "http://xmlns.com/foaf/0.1/firstName"
                                                     "rr:objectMap" {"rr:column" "first_name"}}
                                                    {"rr:predicate" "http://xmlns.com/foaf/0.1/lastName"
                                                     "rr:objectMap" {"rr:column" "last_name"}}]}
          ;; Create a test system with JSON-LD mapping
          test-system-jsonld (system/initialize
                              (config/parse
                               {"@context" {"@base"  "https://ns.flur.ee/config/connection/"
                                            "@vocab" "https://ns.flur.ee/system#"}
                                "@id"      "memory"
                                "@graph"   [{"@id"   "memoryStorage"
                                             "@type" "Storage"}
                                            {"@id"              "connection"
                                             "@type"            "Connection"
                                             "parallelism"      4
                                             "cacheMaxMb"       1000
                                             "commitStorage"    {"@id" "memoryStorage"}
                                             "indexStorage"     {"@id" "memoryStorage"}
                                             "primaryPublisher" {"@type"   "Publisher"
                                                                 "storage" {"@id" "memoryStorage"}}}]}))
          conn-jsonld (some (fn [[k v]] (when (isa? k :fluree.db/connection) v)) test-system-jsonld)
          publisher-jsonld (some (fn [[k v]] (when (isa? k :fluree.db.nameservice/storage) v)) test-system-jsonld)]
      ;; Publish R2RML with JSON-LD mapping
      (async/<!! (nameservice/publish publisher-jsonld {:vg-name "vg/jsonld-sql"
                                                        :vg-type "fidx:R2RML"
                                                        :engine  :r2rml
                                                        :config  {:mappingInline json-ld-mapping
                                                                  :rdb {:jdbcUrl "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1"
                                                                        :driver  "org.h2.Driver"}}
                                                        :dependencies ["dummy-ledger@main"]}))
      ;; Query using the JSON-LD mapping
      (let [query {"from" ["vg/jsonld-sql"]
                   "select" ["?customer" "?firstName" "?lastName"]
                   "where" [["graph" "vg/jsonld-sql" {"@id" "?customer"
                                                      "@type" "http://example.com/Customer"
                                                      "http://xmlns.com/foaf/0.1/firstName" "?firstName"
                                                      "http://xmlns.com/foaf/0.1/lastName" "?lastName"}]]}
            res @(fluree/query-connection conn-jsonld query)
            expected #{["http://example.com/customer/1" "John" "Doe"]
                       ["http://example.com/customer/2" "Jane" "Smith"]
                       ["http://example.com/customer/3" "Bob" "Johnson"]
                       ["http://example.com/customer/4" "Alice" "Brown"]}]
        (is (= expected (set res))
            "Should return correct data using JSON-LD R2RML mapping")))))

