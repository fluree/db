(ns fluree.db.vector.bm25-federated-test
  (:require [clojure.core.async :refer [<!!]]
            [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.util.log :as log]
            [fluree.db.virtual-graph :as vg]))

(deftest ^:integration bm25-federated-query-test
  (testing "Federated queries with ledger and virtual graph using graph syntax"
    (let [conn (test-utils/create-conn)

          ;; Create ledger with documents
          _ledger @(fluree/create conn "library")
          _db @(fluree/insert! conn "library"
                               {"@context" {"lib" "http://library.org/"}
                                "@graph" [{"@id" "lib:book1"
                                           "@type" "lib:Book"
                                           "lib:title" "Graph Databases in Action"
                                           "lib:content" "A comprehensive guide to graph database technology"
                                           "lib:author" {"@id" "lib:author1"}
                                           "lib:year" 2023}
                                          {"@id" "lib:book2"
                                           "@type" "lib:Book"
                                           "lib:title" "Semantic Web Fundamentals"
                                           "lib:content" "Introduction to semantic web and linked data"
                                           "lib:author" {"@id" "lib:author2"}
                                           "lib:year" 2022}
                                          {"@id" "lib:book3"
                                           "@type" "lib:Book"
                                           "lib:title" "Database Design Patterns"
                                           "lib:content" "Modern patterns for database architecture"
                                           "lib:author" {"@id" "lib:author1"}
                                           "lib:year" 2024}
                                          {"@id" "lib:author1"
                                           "@type" "lib:Author"
                                           "lib:name" "Jane Smith"}
                                          {"@id" "lib:author2"
                                           "@type" "lib:Author"
                                           "lib:name" "John Doe"}]})

          ;; Create BM25 virtual graph
          vg-obj @(fluree/create-virtual-graph
                   conn
                   {:name "book-search"
                    :type :bm25
                    :config {:ledgers ["library"]
                             :query {"@context" {"lib" "http://library.org/"}
                                     "where" [{"@id" "?x"
                                               "@type" "lib:Book"}]
                                     "select" {"?x" ["@id" "lib:title" "lib:content"]}}}})
          vg-name (:vg-name vg-obj)]

      (testing "virtual graph created"
        (is (= "book-search" vg-name)))

      ;; Wait for initial indexing
      (<!! (vg/sync vg-obj nil))

      (testing "federated query combining search results with ledger data"
        (let [results @(fluree/query-connection
                        conn
                        {"@context" {"idx" "https://ns.flur.ee/index#"
                                     "lib" "http://library.org/"}
                         "from" ["library" "book-search"]
                         "where" [["graph" "book-search"
                                   {"@id" "?search"
                                    "idx:target" "database"
                                    "idx:limit" 10
                                    "idx:result" {"idx:id" "?book"
                                                  "idx:score" "?score"}}]
                                  ["graph" "library"
                                   {"@id" "?book"
                                    "lib:title" "?title"
                                    "lib:author" "?authorId"
                                    "lib:year" "?year"}]
                                  ["graph" "library"
                                   {"@id" "?authorId"
                                    "lib:name" "?authorName"}]]
                         "select" ["?book" "?title" "?authorName" "?year" "?score"]
                         "orderBy" ["(desc ?score)"]})]

          (log/debug "Federated query results:" results)

          (testing "returns combined results"
            (is (seq results) "Should return results"))

          (testing "results have all fields from both graphs"
            (is (every? #(= 5 (count %)) results) "Each result should have 5 fields"))

          (testing "correct books found"
            (let [book-ids (set (map first results))]
              (is (contains? book-ids "lib:book1") "Should find Graph Databases book")
              (is (contains? book-ids "lib:book3") "Should find Database Design book")))

          (testing "author information is joined correctly"
            (let [result-map (into {} (map (fn [[book _ author _ _]] [book author]) results))]
              (is (= "Jane Smith" (get result-map "lib:book1")) "Book1 should have correct author")
              (is (= "Jane Smith" (get result-map "lib:book3")) "Book3 should have correct author")))))

      (testing "multiple search terms with different graph patterns"
        (let [semantic-results @(fluree/query-connection
                                 conn
                                 {"@context" {"idx" "https://ns.flur.ee/index#"
                                              "lib" "http://library.org/"}
                                  "from" ["library" "book-search"]
                                  "where" [["graph" "book-search"
                                            {"@id" "?search1"
                                             "idx:target" "semantic"
                                             "idx:limit" 5
                                             "idx:result" {"idx:id" "?book"
                                                           "idx:score" "?score"}}]
                                           ["graph" "library"
                                            {"@id" "?book"
                                             "lib:title" "?title"}]]
                                  "select" ["?book" "?title" "?score"]})]

          (testing "semantic search finds correct book"
            (is (seq semantic-results) "Should find semantic-related books")
            (is (some #(= "lib:book2" (first %)) semantic-results) "Should find Semantic Web book"))))

      (testing "aggregation across virtual graph and ledger"
        (let [year-results @(fluree/query-connection
                             conn
                             {"@context" {"idx" "https://ns.flur.ee/index#"
                                          "lib" "http://library.org/"}
                              "from" ["library" "book-search"]
                              "where" [["graph" "book-search"
                                        {"@id" "?search"
                                         "idx:target" "database"
                                         "idx:limit" 20
                                         "idx:result" {"idx:id" "?book"}}]
                                       ["graph" "library"
                                        {"@id" "?book"
                                         "lib:year" "?year"}]]
                              "select" ["?year" "(as (count ?book) ?bookCount)"]
                              "groupBy" ["?year"]
                              "orderBy" ["?year"]})]

          (log/debug "Year aggregation results:" year-results)

          (testing "aggregation works across graphs"
            (is (seq year-results) "Should return aggregated results")
            (is (every? #(= 2 (count %)) year-results) "Each result should have year and count")))))))
