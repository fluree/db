(ns fluree.db.virtual-graph.r2rml-test
  (:require [clojure.core.async :as async]
            [clojure.java.jdbc :as jdbc]
            [clojure.test :refer [deftest is testing use-fixtures]]
            [fluree.db.api :as fluree]
            [fluree.db.connection.config :as config]
            [fluree.db.connection.system :as system]
            [fluree.db.nameservice :as nameservice]
            [fluree.db.util.async :refer [<?]]))

(def h2-spec {:dbtype "h2:mem"
              :dbname "r2rml_test;DB_CLOSE_DELAY=-1"})

(def create-sql ["CREATE TABLE people (id INT PRIMARY KEY, name VARCHAR(100));"
                 "INSERT INTO people (id, name) VALUES (1, 'Alice');"
                 "INSERT INTO people (id, name) VALUES (2, 'Bob');"
                 "CREATE TABLE customers (customer_id INT PRIMARY KEY, first_name VARCHAR(100), last_name VARCHAR(100), email VARCHAR(100));"
                 "INSERT INTO customers (customer_id, first_name, last_name, email) VALUES (1, 'John', 'Doe', 'john@example.com');"
                 "INSERT INTO customers (customer_id, first_name, last_name, email) VALUES (2, 'Jane', 'Smith', 'jane@example.com');"
                 "CREATE TABLE orders (order_id INT PRIMARY KEY, customer_id INT, order_date DATE, total_amount DECIMAL(10,2));"
                 "INSERT INTO orders (order_id, customer_id, order_date, total_amount) VALUES (101, 1, '2024-01-15', 150.00);"
                 "INSERT INTO orders (order_id, customer_id, order_date, total_amount) VALUES (102, 2, '2024-01-16', 200.00);"])

(def r2rml-ttl (str "@prefix rr: <http://www.w3.org/ns/r2rml#> .\n"
                    "@prefix ex: <http://example.org/> .\n"
                    "@prefix schema: <http://schema.org/> .\n"
                    "@prefix foaf: <http://xmlns.com/foaf/0.1/> .\n"
                    "@prefix dcterms: <http://purl.org/dc/terms/> .\n"
                    "ex:PeopleMap a rr:TriplesMap;\n"
                    "  rr:logicalTable [ rr:tableName \"PEOPLE\" ];\n"
                    "  rr:subjectMap [ rr:template \"http://example.org/person/{ID}\"; rr:termType rr:IRI ];\n"
                    "  rr:predicateObjectMap [ rr:predicate schema:name; rr:objectMap [ rr:column \"NAME\" ] ] .\n"
                    "ex:CustomersMap a rr:TriplesMap;\n"
                    "  rr:logicalTable [ rr:tableName \"customers\" ];\n"
                    "  rr:subjectMap [ rr:template \"http://example.org/customer/{customer_id}\"; rr:termType rr:IRI ];\n"
                    "  rr:predicateObjectMap [ rr:predicate foaf:firstName; rr:objectMap [ rr:column \"first_name\" ] ];\n"
                    "  rr:predicateObjectMap [ rr:predicate foaf:lastName; rr:objectMap [ rr:column \"last_name\" ] ];\n"
                    "  rr:predicateObjectMap [ rr:predicate foaf:mbox; rr:objectMap [ rr:column \"email\" ] ] .\n"
                    "ex:OrdersMap a rr:TriplesMap;\n"
                    "  rr:logicalTable [ rr:tableName \"orders\" ];\n"
                    "  rr:subjectMap [ rr:template \"http://example.org/order/{order_id}\"; rr:termType rr:IRI ];\n"
                    "  rr:predicateObjectMap [ rr:predicate ex:customer; rr:objectMap [ rr:template \"http://example.org/customer/{customer_id}\" ] ];\n"
                    "  rr:predicateObjectMap [ rr:predicate dcterms:date; rr:objectMap [ rr:column \"order_date\" ] ];\n"
                    "  rr:predicateObjectMap [ rr:predicate ex:totalAmount; rr:objectMap [ rr:column \"total_amount\" ] ] .\n"))

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
                                                         :rdb {:jdbcUrl (str "jdbc:h2:mem:" (:dbname h2-spec))
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
                                         "http://example.org/totalAmount" "?totalAmount"}]]}]
    (let [res @(fluree/query-connection conn query)
          amounts (set (map second res))]
      (is (contains? amounts 150.00M))
      (is (contains? amounts 200.00M)))))


