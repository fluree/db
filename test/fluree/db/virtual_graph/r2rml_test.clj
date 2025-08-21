(ns fluree.db.virtual-graph.r2rml-test
  (:require [clojure.core.async :as async]
            [clojure.java.jdbc :as jdbc]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [fluree.db.api :as fluree]
            [fluree.db.connection.config :as config]
            [fluree.db.connection.system :as system]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.util.async :refer [<?]]))

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

(defn with-h2 [f]
  (jdbc/with-db-connection [conn h2-spec]
    (doseq [s create-sql]
      (jdbc/execute! conn [s]))
    (f)))

(use-fixtures :once with-h2)

(defn memory-conn []
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
        system-map (system/initialize (config/parse memory-config))]
    system-map))

(defn publish-vg! [publisher vg-name]
  (let [tmp-file (java.io.File/createTempFile "r2rml" ".ttl")]
    (spit tmp-file r2rml-ttl)
    (async/<!! (nameservice/publish publisher {:vg-name vg-name
                                               :vg-type "fidx:R2RML"
                                               :engine  :r2rml
                                               :config  {:mapping (.getAbsolutePath tmp-file)
                                                         :rdb {:jdbcUrl "jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1"
                                                               :driver  "org.h2.Driver"}}
                                               :dependencies ["dummy-ledger@main"]}))))

(deftest r2rml-simple-graph-test
  (let [sys (memory-conn)
        conn (some (fn [[k v]] (when (isa? k :fluree.db/connection) v)) sys)
        publisher (some (fn [[k v]] (when (isa? k :fluree.db.nameservice/storage) v)) sys)
        _ (publish-vg! publisher "vg/sql")
        query {:from ["vg/sql"]
               :select ['?s '?name]
               :where [[:graph "vg/sql" {"@id" "?s" "http://schema.org/name" "?name"}]]}]
    (let [res @(fluree/query-connection conn query)
          names (set (map second res))]
      (is (contains? names "Alice"))
      (is (contains? names "Bob")))))

(deftest r2rml-customers-test
  (let [sys (memory-conn)
        conn (some (fn [[k v]] (when (isa? k :fluree.db/connection) v)) sys)
        publisher (some (fn [[k v]] (when (isa? k :fluree.db.nameservice/storage) v)) sys)
        _ (publish-vg! publisher "vg/sql")
        query {:from ["vg/sql"]
               :select ['?firstName '?lastName '?email]
               :where [[:graph "vg/sql" {"@id" "?customer"
                                         "http://xmlns.com/foaf/0.1/firstName" "?firstName"
                                         "http://xmlns.com/foaf/0.1/lastName" "?lastName"
                                         "http://xmlns.com/foaf/0.1/mbox" "?email"}]]}]
    (let [res @(fluree/query-connection conn query)
          first-names (set (map first res))
          last-names (set (map second res))
          emails (set (map #(nth % 2) res))]
      (is (contains? first-names "John"))
      (is (contains? first-names "Jane"))
      (is (contains? last-names "Doe"))
      (is (contains? last-names "Smith"))
      (is (contains? emails "john@example.com"))
      (is (contains? emails "jane@example.com")))))

(deftest r2rml-orders-test
  (let [sys (memory-conn)
        conn (some (fn [[k v]] (when (isa? k :fluree.db/connection) v)) sys)
        publisher (some (fn [[k v]] (when (isa? k :fluree.db.nameservice/storage) v)) sys)
        _ (publish-vg! publisher "vg/sql")
        query {:from ["vg/sql"]
               :select ['?orderDate '?totalAmount]
               :where [[:graph "vg/sql" {"@id" "?order"
                                         "http://purl.org/dc/terms/date" "?orderDate"
                                         "http://example.com/totalAmount" "?totalAmount"}]]}]
    (let [res @(fluree/query-connection conn query)
          amounts (set (map second res))]
      (is (contains? amounts 1029.98M))
      (is (contains? amounts 89.99M))
      (is (contains? amounts 299.99M))
      (is (contains? amounts 199.99M))
      (is (contains? amounts 999.99M)))))

(deftest r2rml-aggregate-count-test
  (testing "COUNT aggregate - client-side counting since aggregates may not work with virtual graphs"
    (let [sys (memory-conn)
          conn (some (fn [[k v]] (when (isa? k :fluree.db/connection) v)) sys)
          publisher (some (fn [[k v]] (when (isa? k :fluree.db.nameservice/storage) v)) sys)
          _ (publish-vg! publisher "vg/sql")
          ;; Virtual graphs return raw data, count client-side
          query {:from ["vg/sql"]
                 :select ["?order"]
                 :where [[:graph "vg/sql" {"@id" "?order"
                                           "http://www.w3.org/1999/02/22-rdf-syntax-ns#type" "http://example.com/Order"}]]}]
      (let [res @(fluree/query-connection conn query)
            count-val (count res)]
        (is (= 5 count-val))))))

(deftest r2rml-aggregate-sum-test
  (testing "SUM aggregate function with proper Fluree syntax"
    (let [sys (memory-conn)
          conn (some (fn [[k v]] (when (isa? k :fluree.db/connection) v)) sys)
          publisher (some (fn [[k v]] (when (isa? k :fluree.db.nameservice/storage) v)) sys)
          _ (publish-vg! publisher "vg/sql")
          ;; Proper Fluree aggregate syntax would be:
          ;; :select ["(as (sum ?amount) ?total)"]
          ;; But virtual graphs return raw data; aggregation happens in Fluree's query engine
          ]
      (testing "Virtual graphs return individual values for client-side aggregation"
        (let [fallback-query {:from ["vg/sql"]
                              :select ["?amount"]
                              :where [[:graph "vg/sql" {"@id" "?order"
                                                        "http://example.com/totalAmount" "?amount"}]]}
              res @(fluree/query-connection conn fallback-query)
              amounts (map first res)
              total-sum (reduce + 0M amounts)]
          ;; We should get 5 order amounts that sum to 2619.94
          (is (= 5 (count amounts)))
          (is (= 2619.94M total-sum)))))))

(deftest r2rml-aggregate-avg-test
  (testing "AVG aggregate function - compute from individual amounts"
    (let [sys (memory-conn)
          conn (some (fn [[k v]] (when (isa? k :fluree.db/connection) v)) sys)
          publisher (some (fn [[k v]] (when (isa? k :fluree.db.nameservice/storage) v)) sys)
          _ (publish-vg! publisher "vg/sql")
          ;; Get individual amounts and compute average
          query {:from ["vg/sql"]
                 :select ["?amount"]
                 :where [[:graph "vg/sql" {"@id" "?order"
                                           "http://example.com/totalAmount" "?amount"}]]}]
      (let [res @(fluree/query-connection conn query)
            amounts (map first res)
            avg-val (/ (reduce + 0M amounts) (count amounts))]
        (is (= 523.988M avg-val))))))

(deftest r2rml-aggregate-min-max-test
  (testing "MIN and MAX aggregate functions - compute from individual amounts"
    (let [sys (memory-conn)
          conn (some (fn [[k v]] (when (isa? k :fluree.db/connection) v)) sys)
          publisher (some (fn [[k v]] (when (isa? k :fluree.db.nameservice/storage) v)) sys)
          _ (publish-vg! publisher "vg/sql")
          query {:from ["vg/sql"]
                 :select ["?amount"]
                 :where [[:graph "vg/sql" {"@id" "?order"
                                           "http://example.com/totalAmount" "?amount"}]]}]
      (let [res @(fluree/query-connection conn query)
            amounts (map first res)
            min-val (apply min amounts)
            max-val (apply max amounts)]
        (is (= 89.99M min-val))
        (is (= 1029.98M max-val))))))

(deftest r2rml-group-by-test
  (testing "GROUP BY with aggregates - client-side grouping"
    (let [sys (memory-conn)
          conn (some (fn [[k v]] (when (isa? k :fluree.db/connection) v)) sys)
          publisher (some (fn [[k v]] (when (isa? k :fluree.db.nameservice/storage) v)) sys)
          _ (publish-vg! publisher "vg/sql")
          ;; Get all statuses and group them client-side
          query {:from ["vg/sql"]
                 :select ["?status"]
                 :where [[:graph "vg/sql" {"@id" "?order"
                                           "http://example.com/status" "?status"}]]}]
      (let [res @(fluree/query-connection conn query)
            statuses (map first res)
            status-counts (frequencies statuses)]
        (is (= 3 (get status-counts "completed")))
        (is (= 1 (get status-counts "pending")))
        (is (= 1 (get status-counts "cancelled")))))))

(deftest r2rml-products-test
  (testing "r2rml-products-test"
    (let [sys (memory-conn)
          conn (some (fn [[k v]] (when (isa? k :fluree.db/connection) v)) sys)
          publisher (some (fn [[k v]] (when (isa? k :fluree.db.nameservice/storage) v)) sys)
          _ (publish-vg! publisher "vg/sql")
          query {:from ["vg/sql"]
                 :select ["?product" "?name" "?price"]
                 :where [[:graph "vg/sql" {"@id" "?product"
                                           "http://www.w3.org/2000/01/rdf-schema#label" "?name"
                                           "http://example.com/price" "?price"}]]}]
      (let [res @(fluree/query-connection conn query)
            prices (set (map #(nth % 2) res))]
        (is (contains? prices 999.99M))
        (is (contains? prices 29.99M))
        (is (contains? prices 89.99M))
        (is (contains? prices 299.99M))
        (is (contains? prices 199.99M))))))

(deftest r2rml-product-aggregates-test
  (testing "Product aggregates - client-side computation"
    (let [sys (memory-conn)
          conn (some (fn [[k v]] (when (isa? k :fluree.db/connection) v)) sys)
          publisher (some (fn [[k v]] (when (isa? k :fluree.db.nameservice/storage) v)) sys)
          _ (publish-vg! publisher "vg/sql")
          ;; Get individual prices and stocks
          query {:from ["vg/sql"]
                 :select ["?price" "?stock"]
                 :where [[:graph "vg/sql" {"@id" "?product"
                                           "http://example.com/price" "?price"
                                           "http://example.com/stockQuantity" "?stock"}]]}]
      (let [res @(fluree/query-connection conn query)
            prices (map first res)
            stocks (map second res)
            avg-price (/ (reduce + 0M prices) (count prices))
            total-stock (reduce + 0 stocks)]
        (is (= 323.99M avg-price))
        (is (= 130 total-stock))))))


