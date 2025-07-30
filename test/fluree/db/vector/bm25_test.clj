(ns fluree.db.vector.bm25-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.util.log :as log]))

(deftest ^:integration bm25-basic-functionality-test
  (testing "Basic BM25 virtual graph functionality"
    (let [conn   (test-utils/create-conn)
          _ledger @(fluree/create conn "bm25-basic")
          _db     @(fluree/insert! conn "bm25-basic"
                                   {"@context" {"ex" "http://example.org/"}
                                    "@graph" [{"@id" "ex:doc1"
                                               "@type" "ex:Document"
                                               "ex:title" "Introduction to Fluree"
                                               "ex:content" "Fluree is a semantic graph database"}]})
          ;; Create VG
          vg-name @(fluree/create-virtual-graph
                    conn
                    {:name "basic-search"
                     :type :bm25
                     :config {:ledgers ["bm25-basic"]
                              :query {"@context" {"ex" "http://example.org/"}
                                      "where" [{"@id" "?x"
                                                "@type" "ex:Document"}]
                                      "select" {"?x" ["@id" "ex:title" "ex:content"]}}}})]

      (testing "virtual graph creation succeeds"
        (is (= "basic-search" vg-name)))

      ;; Wait for indexing
      (Thread/sleep 5000)

      (testing "search functionality"
        (let [results @(fluree/query-connection conn
                                                {"@context" {"idx" "https://ns.flur.ee/index#"}
                                                 "from" ["basic-search"]
                                                 "where" [{"@id" "?x"
                                                           "idx:target" "fluree"
                                                           "idx:limit" 10
                                                           "idx:result" {"idx:id" "?doc"
                                                                         "idx:score" "?score"}}]
                                                 "select" ["?doc" "?score"]})]
          (println "Search results:" results)
          (is (seq results) "Should return search results")
          (when (seq results)
            (println "First result:" (first results))
            (is (= "http://example.org/doc1" (ffirst results)) "Should find the document")
            (is (number? (second (first results))) "Should have numeric score")))))))
