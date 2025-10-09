(ns fluree.db.query.stream-test
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.query.api :as query-api]
            [fluree.db.test-utils :as test-utils]))

(deftest basic-streaming-query-test
  (testing "Basic streaming query returns individual results"
    (let [conn      (test-utils/create-conn)
          ledger-id "query/stream"
          db0       @(fluree/create conn ledger-id)
          db1       @(fluree/update
                      db0
                      {"@context" {"ex" "http://example.org/"}
                       "insert"   [{"@id"     "ex:alice"
                                    "@type"   "ex:Person"
                                    "ex:name" "Alice"
                                    "ex:age"  30}
                                   {"@id"     "ex:bob"
                                    "@type"   "ex:Person"
                                    "ex:name" "Bob"
                                    "ex:age"  25}
                                   {"@id"     "ex:charlie"
                                    "@type"   "ex:Person"
                                    "ex:name" "Charlie"
                                    "ex:age"  35}]})
          query     {"@context" {"ex" "http://example.org/"}
                     "select"   ["?name" "?age"]
                     "where"    [{"@id"     "?person"
                                  "@type"   "ex:Person"
                                  "ex:name" "?name"
                                  "ex:age"  "?age"}]}
          result-ch (query-api/query-stream db1 query {})
          results   (async/<!! (async/into [] result-ch))]

      (is (= 3 (count results)) "Should return 3 individual results")
      (is (every? vector? results) "Each result should be a vector")
      (is (every? #(= 2 (count %)) results) "Each result should have 2 elements")
      (is (= ["Alice" "Bob" "Charlie"]
             (mapv first results))
          "Should return names in insertion order"))))

(deftest streaming-with-limit-test
  (testing "Streaming query with LIMIT"
    (let [conn      (test-utils/create-conn)
          ledger-id "query/stream-limit"
          db0       @(fluree/create conn ledger-id)
          db1       @(fluree/update
                      db0
                      {"@context" {"ex" "http://example.org/"}
                       "insert"   [{"@id"     "ex:person1"
                                    "ex:name" "Person 1"}
                                   {"@id"     "ex:person2"
                                    "ex:name" "Person 2"}
                                   {"@id"     "ex:person3"
                                    "ex:name" "Person 3"}
                                   {"@id"     "ex:person4"
                                    "ex:name" "Person 4"}
                                   {"@id"     "ex:person5"
                                    "ex:name" "Person 5"}]})
          query     {"@context" {"ex" "http://example.org/"}
                     "select"   ["?name"]
                     "where"    [{"@id"     "?person"
                                  "ex:name" "?name"}]
                     "limit"    2}
          result-ch (query-api/query-stream db1 query {})
          results   (async/<!! (async/into [] result-ch))]

      (is (= 2 (count results)) "Should return only 2 results due to LIMIT"))))

(deftest streaming-select-one-test
  (testing "Streaming SELECT ONE query"
    (let [conn      (test-utils/create-conn)
          ledger-id "query/stream-one"
          db0       @(fluree/create conn ledger-id)
          db1       @(fluree/update
                      db0
                      {"@context" {"ex" "http://example.org/"}
                       "insert"   [{"@id"     "ex:alice"
                                    "ex:name" "Alice"}
                                   {"@id"     "ex:bob"
                                    "ex:name" "Bob"}]})
          query     {"@context"  {"ex" "http://example.org/"}
                     "selectOne" ["?name"]
                     "where"     [{"@id"     "?person"
                                   "ex:name" "?name"}]}
          result-ch (query-api/query-stream db1 query {})
          results   (async/<!! (async/into [] result-ch))]

      (is (= 1 (count results)) "SELECT ONE should return only one result")
      (is (vector? (first results)) "Result should be a vector"))))

(deftest streaming-with-meta-tracking-test
  (testing "Streaming query with :meta tracking emits final metadata"
    (let [conn      (test-utils/create-conn)
          ledger-id "query/stream-meta"
          db0       @(fluree/create conn ledger-id)
          db1       @(fluree/update
                      db0
                      {"@context" {"ex" "http://example.org/"}
                       "insert"   [{"@id"     "ex:alice"
                                    "ex:name" "Alice"}
                                   {"@id"     "ex:bob"
                                    "ex:name" "Bob"}]})
          query     {"@context" {"ex" "http://example.org/"}
                     "select"   ["?name"]
                     "where"    [{"@id"     "?person"
                                  "ex:name" "?name"}]}
          result-ch (query-api/query-stream db1 query {:meta true})
          results   (async/<!! (async/into [] result-ch))]

      (is (= 3 (count results)) "Should return 2 results + 1 metadata map")
      (is (vector? (first results)) "First result should be a vector")
      (is (vector? (second results)) "Second result should be a vector")

      (let [meta-map (last results)]
        (is (map? meta-map) "Last result should be a map")
        (is (contains? meta-map :_fluree-meta) "Should contain :_fluree-meta key")
        (is (= 200 (get-in meta-map [:_fluree-meta :status])) "Metadata should have status 200")
        (is (contains? (get meta-map :_fluree-meta) :time) "Metadata should include :time")
        (is (contains? (get meta-map :_fluree-meta) :fuel) "Metadata should include :fuel")))))

(deftest streaming-connection-query-test
  (testing "Streaming query via connection"
    (let [conn      (test-utils/create-conn)
          ledger-id "stream/conn"
          db0       @(fluree/create conn ledger-id)
          _         @(fluree/commit! conn
                                     @(fluree/update
                                       db0
                                       {"@context" {"ex" "http://example.org/"}
                                        "insert"   [{"@id"     "ex:alice"
                                                     "ex:name" "Alice"}
                                                    {"@id"     "ex:bob"
                                                     "ex:name" "Bob"}]}))
          query     {"@context" {"ex" "http://example.org/"}
                     "from"     ledger-id
                     "select"   ["?name"]
                     "where"    [{"@id"     "?person"
                                  "ex:name" "?name"}]}
          result-ch (query-api/query-connection-stream conn query {})
          results   (async/<!! (async/into [] result-ch))]

      (is (= 2 (count results)) "Should return 2 results")
      (is (every? vector? results) "Each result should be a vector")
      (is (= ["Alice" "Bob"]
             (mapv first results))
          "Should return both names in order"))))

(deftest streaming-connection-with-meta-test
  (testing "Streaming connection query with :meta tracking"
    (let [conn      (test-utils/create-conn)
          ledger-id "stream/conn-meta"
          db0       @(fluree/create conn ledger-id)
          _         @(fluree/commit! conn
                                     @(fluree/update
                                       db0
                                       {"@context" {"ex" "http://example.org/"}
                                        "insert"   [{"@id"     "ex:alice"
                                                     "ex:name" "Alice"}
                                                    {"@id"     "ex:bob"
                                                     "ex:name" "Bob"}]}))
          query     {"@context" {"ex" "http://example.org/"}
                     "from"     ledger-id
                     "select"   ["?name"]
                     "where"    [{"@id"     "?person"
                                  "ex:name" "?name"}]}
          result-ch (query-api/query-connection-stream conn query {:meta true})
          results   (async/<!! (async/into [] result-ch))]

      (is (= 3 (count results)) "Should return 2 results + 1 metadata map")
      (is (every? vector? (take 2 results)) "First two results should be vectors")

      (let [meta-map (last results)]
        (is (map? meta-map) "Last result should be a map")
        (is (contains? meta-map :_fluree-meta) "Should contain :_fluree-meta key")
        (is (= 200 (get-in meta-map [:_fluree-meta :status])) "Metadata should have status 200")))))

(deftest streaming-sparql-query-test
  (testing "Streaming SPARQL query returns individual results"
    (let [conn      (test-utils/create-conn)
          ledger-id "query/sparql-stream"
          db0       @(fluree/create conn ledger-id)
          db1       @(fluree/update
                      db0
                      {"@context" {"ex" "http://example.org/"}
                       "insert"   [{"@id"     "ex:alice"
                                    "ex:name" "Alice"
                                    "ex:age"  30}
                                   {"@id"     "ex:bob"
                                    "ex:name" "Bob"
                                    "ex:age"  25}]})
          sparql-query "SELECT ?name ?age WHERE { ?person <http://example.org/name> ?name . ?person <http://example.org/age> ?age }"
          result-ch    (query-api/query-stream db1 sparql-query {:format :sparql})
          results      (async/<!! (async/into [] result-ch))]

      (is (= 2 (count results)) "Should return 2 individual results")
      (is (every? vector? results) "Each result should be a vector"))))

(deftest streaming-sparql-with-meta-test
  (testing "Streaming SPARQL query with :meta tracking"
    (let [conn      (test-utils/create-conn)
          ledger-id "query/sparql-stream-meta"
          db0       @(fluree/create conn ledger-id)
          db1       @(fluree/update
                      db0
                      {"@context" {"ex" "http://example.org/"}
                       "insert"   [{"@id"     "ex:alice"
                                    "ex:name" "Alice"}
                                   {"@id"     "ex:bob"
                                    "ex:name" "Bob"}]})
          sparql-query "SELECT ?name WHERE { ?person <http://example.org/name> ?name }"
          result-ch    (query-api/query-stream db1 sparql-query {:format :sparql :meta true})
          results      (async/<!! (async/into [] result-ch))]

      (is (= 3 (count results)) "Should return 2 results + 1 metadata map")
      (is (every? vector? (take 2 results)) "First two results should be vectors")

      (let [meta-map (last results)]
        (is (map? meta-map) "Last result should be a map")
        (is (contains? meta-map :_fluree-meta) "Should contain :_fluree-meta key")
        (is (= 200 (get-in meta-map [:_fluree-meta :status])) "Metadata should have status 200")))))

(deftest streaming-construct-query-test
  (testing "Streaming CONSTRUCT query returns individual graph nodes"
    (let [conn      (test-utils/create-conn)
          ledger-id "query/construct-stream"
          db0       @(fluree/create conn ledger-id)
          db1       @(fluree/update
                      db0
                      {"@context" {"ex" "http://example.org/"}
                       "insert"   [{"@id"       "ex:alice"
                                    "ex:name"   "Alice"
                                    "ex:hobby"  ["reading" "hiking"]}
                                   {"@id"      "ex:bob"
                                    "ex:name"  "Bob"
                                    "ex:hobby" ["swimming"]}]})
          query     {"@context" {"ex" "http://example.org/"}
                     "where"    [{"@id"      "?person"
                                  "ex:name"  "?name"}
                                 {"@id"      "?person"
                                  "ex:hobby" "?hobby"}]
                     "construct" [{"@id"    "?person"
                                   "label"  "?name"}
                                  {"@id"    "?person"
                                   "hobby"  "?hobby"}]}
          result-ch (query-api/query-stream db1 query {})
          results   (async/<!! (async/into [] result-ch))]

      ;; Alice has 2 hobbies + Bob has 1 hobby = 3 WHERE solutions
      ;; Each solution produces 2 CONSTRUCT templates = 6 graph nodes total
      (is (>= (count results) 3) "Should return at least 3 individual graph nodes")
      (is (every? map? results) "Each result should be a map")
      (is (every? #(contains? % "@id") results) "Each result should have @id")
      (is (not-any? #(contains? % "@graph") results) "Results should NOT be wrapped in @graph")
      (is (>= (count results) 3) "Should stream multiple individual graph nodes"))))

(deftest streaming-sparql-construct-test
  (testing "Streaming SPARQL CONSTRUCT query returns individual graph nodes"
    (let [conn      (test-utils/create-conn)
          ledger-id "query/sparql-construct-stream"
          db0       @(fluree/create conn ledger-id)
          db1       @(fluree/update
                      db0
                      {"@context" {"ex" "http://example.org/"}
                       "insert"   [{"@id"       "ex:alice"
                                    "ex:name"   "Alice"
                                    "ex:hobby"  ["reading" "hiking"]}
                                   {"@id"      "ex:bob"
                                    "ex:name"  "Bob"
                                    "ex:hobby" ["swimming"]}]})
          sparql-query "CONSTRUCT { ?person <http://example.org/label> ?name . ?person <http://example.org/hobby> ?hobby }
                        WHERE { ?person <http://example.org/name> ?name . ?person <http://example.org/hobby> ?hobby }"
          result-ch    (query-api/query-stream db1 sparql-query {:format :sparql})
          results      (async/<!! (async/into [] result-ch))]

      (is (>= (count results) 3) "Should return at least 3 individual graph nodes")
      (is (every? map? results) "Each result should be a map")
      (is (every? #(contains? % "@id") results) "Each result should have @id")
      (is (not-any? #(contains? % "@graph") results) "Results should NOT be wrapped in @graph"))))
