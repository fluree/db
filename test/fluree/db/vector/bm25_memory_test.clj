(ns fluree.db.vector.bm25-memory-test
  (:require [clojure.core.async :refer [<!!]]
            [clojure.string :as str]
            [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.util.log :as log]
            [fluree.db.virtual-graph :as vg]))

(deftest ^:integration bm25-memory-search-test
  (testing "BM25 virtual graph creation and search with memory storage"
    (let [conn @(fluree/connect-memory)

          ;; Create ledger and add documents
          _ledger @(fluree/create conn "docs")
          _db @(fluree/insert! conn "docs"
                               {"@context" {"ex" "http://example.org/"}
                                "@graph" [{"@id" "ex:article1"
                                           "@type" "ex:Article"
                                           "ex:title" "Introduction to Fluree Database"
                                           "ex:content" "Fluree is a semantic graph database with blockchain properties"}
                                          {"@id" "ex:article2"
                                           "@type" "ex:Article"
                                           "ex:title" "Advanced Query Patterns"
                                           "ex:content" "Learn about complex query patterns in graph databases"}
                                          {"@id" "ex:article3"
                                           "@type" "ex:Article"
                                           "ex:title" "Blockchain Integration"
                                           "ex:content" "How Fluree integrates blockchain technology with databases"}]})

          ;; Create BM25 virtual graph
          vg-obj @(fluree/create-virtual-graph
                   conn
                   {:name "doc-search"
                    :type :bm25
                    :config {:ledgers ["docs"]
                             :query {"@context" {"ex" "http://example.org/"}
                                     "where" [{"@id" "?x"
                                               "@type" "ex:Article"}]
                                     "select" {"?x" ["@id" "ex:title" "ex:content"]}}}})
          vg-name (:vg-name vg-obj)]

      (testing "virtual graph creation returns correct name"
        (is (= "doc-search:main" vg-name)))

      ;; Wait for initial indexing using sync method
      (<!! (vg/sync vg-obj nil))

      (testing "direct virtual graph query for search"
        (let [search-results @(fluree/query-connection
                               conn
                               {"@context" {"idx" "https://ns.flur.ee/index#"
                                            "ex" "http://example.org/"}
                                "from" ["doc-search"]
                                "where" [{"@id" "?x"
                                          "idx:target" "fluree"
                                          "idx:limit" 10
                                          "idx:result" {"idx:id" "?article"
                                                        "idx:score" "?score"}}]
                                "select" ["?article" "?score"]})]

          (log/debug "Search results for 'fluree':" search-results)

          (testing "search returns results"
            (is (seq search-results) "Should return search results"))

          (testing "results contain expected articles"
            (let [article-ids (set (map first search-results))]
              (is (contains? article-ids "ex:article1")
                  "Should find article1 mentioning 'Fluree'")
              (is (contains? article-ids "ex:article3")
                  "Should find article3 mentioning 'Fluree'")))

          (testing "results include BM25 scores"
            (is (every? #(number? (second %)) search-results) "All results should have numeric scores"))

          (testing "scores are properly ordered"
            (let [scores (map second search-results)]
              (is (= scores (sort > scores)) "Scores should be in descending order")))))

      (testing "blank search target returns empty results"
        (let [empty-results @(fluree/query-connection
                              conn
                              {"@context" {"idx" "https://ns.flur.ee/index#"}
                               "from" ["doc-search"]
                               "where" [{"@id" "?x"
                                         "idx:target" ""
                                         "idx:limit" 10
                                         "idx:result" {"idx:id" "?article"}}]
                               "select" ["?article"]})]
          (is (vector? empty-results) "Should return a vector of results")
          (is (empty? empty-results) "Blank target should return no results")))

      (testing "federated query with ledger and virtual graph"
        (let [federated-results @(fluree/query-connection
                                  conn
                                  {"@context" {"idx" "https://ns.flur.ee/index#"
                                               "ex" "http://example.org/"}
                                   "from" ["docs" "doc-search"]
                                   "where" [["graph" "doc-search"
                                             {"@id" "?x"
                                              "idx:target" "database"
                                              "idx:limit" 5
                                              "idx:result" {"idx:id" "?article"
                                                            "idx:score" "?score"}}]
                                            ["graph" "docs"
                                             {"@id" "?article"
                                              "ex:title" "?title"}]]
                                   "select" ["?article" "?title" "?score"]})]

          (log/debug "Federated query results:" federated-results)

          (testing "federated query returns results"
            (is (seq federated-results) "Should return federated results"))

          (testing "results join data from both graphs"
            (is (every? #(= 3 (count %)) federated-results) "Each result should have article, title, and score"))

          (testing "titles match expected articles"
            (let [titles (set (map second federated-results))]
              (is (some #(re-find #"Fluree" %) titles) "Should find titles containing 'Fluree'")
              (is (some #(re-find #"database" (str/lower-case %)) titles) "Should find titles about databases")))))

      (testing "search with different terms"
        (let [blockchain-results @(fluree/query-connection
                                   conn
                                   {"@context" {"idx" "https://ns.flur.ee/index#"}
                                    "from" ["doc-search"]
                                    "where" [{"@id" "?x"
                                              "idx:target" "blockchain"
                                              "idx:limit" 10
                                              "idx:result" {"idx:id" "?article"
                                                            "idx:score" "?score"}}]
                                    "select" ["?article" "?score"]})]

          (testing "blockchain search returns results"
            (is (seq blockchain-results) "Should find articles about blockchain"))

          (testing "blockchain search finds correct article"
            (let [article-ids (set (map first blockchain-results))]
              (is (contains? article-ids "http://example.org/article3")
                  "Should find article3 about blockchain"))))))))
