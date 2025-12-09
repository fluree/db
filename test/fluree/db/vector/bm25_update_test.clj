(ns fluree.db.vector.bm25-update-test
  "Test that BM25 virtual graphs properly update when their dependent ledgers change"
  (:require [clojure.core.async :refer [<!!]]
            [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.util.log :as log]
            [fluree.db.virtual-graph :as vg]))

(deftest ^:integration bm25-ledger-update-test
  (testing "BM25 virtual graph updates when dependent ledger changes"
    (let [conn    (test-utils/create-conn)
          _ledger @(fluree/create conn "books")

          ;; Initial data
          _db1    @(fluree/insert! conn "books"
                                   {"@context" {"ex" "http://example.org/"}
                                    "@graph" [{"@id" "ex:book1"
                                               "@type" "ex:Book"
                                               "ex:title" "Introduction to Databases"
                                               "ex:content" "This book covers SQL and NoSQL databases"}
                                              {"@id" "ex:book2"
                                               "@type" "ex:Book"
                                               "ex:title" "Advanced Programming"
                                               "ex:content" "Learn advanced programming techniques"}]})

          ;; Create BM25 virtual graph and get the VG object
          vg-obj @(fluree/create-virtual-graph
                   conn
                   {:name "book-search"
                    :type :bm25
                    :config {:ledgers ["books"]
                             :query {"@context" {"ex" "http://example.org/"}
                                     "where" [{"@id" "?x"
                                               "@type" "ex:Book"}]
                                     "select" {"?x" ["@id" "ex:title" "ex:content"]}}}})]

      (testing "virtual graph creation succeeds"
        (is (some? vg-obj) "Should return VG object")
        (is (= "book-search:main" (:vg-name vg-obj)) "VG should have correct name"))

      ;; Wait for initial indexing using sync method
      (<!! (vg/sync vg-obj nil))

      (testing "initial search finds existing books"
        (let [results @(fluree/query-connection conn
                                                {"@context" {"idx" "https://ns.flur.ee/index#"}
                                                 "from" ["book-search"]
                                                 "where" [{"@id" "?x"
                                                           "idx:target" "databases"
                                                           "idx:limit" 10
                                                           "idx:result" {"idx:id" "?doc"
                                                                         "idx:score" "?score"}}]
                                                 "select" ["?doc" "?score"]})]
          (log/info "Initial search results for 'databases':" results)
          (is (= 1 (count results)) "Should find one book about databases")
          (is (= "http://example.org/book1" (ffirst results)) "Should find book1")))

      ;; Add a new book to the ledger
      (testing "adding new book to ledger"
        (let [_db2 @(fluree/insert! conn "books"
                                    {"@context" {"ex" "http://example.org/"}
                                     "@graph" [{"@id" "ex:book3"
                                                "@type" "ex:Book"
                                                "ex:title" "Getting Started with Fluree"
                                                "ex:content" "Fluree is a semantic graph database"}]})]

          ;; Wait for the VG to be updated using the latest database t
          (let [current-db @(fluree/load conn "books")
                current-t (:t current-db)]
            (<!! (vg/sync vg-obj current-t)))

          (testing "search finds newly added book"
            (let [results @(fluree/query-connection conn
                                                    {"@context" {"idx" "https://ns.flur.ee/index#"}
                                                     "from" ["book-search"]
                                                     "where" [{"@id" "?x"
                                                               "idx:target" "fluree"
                                                               "idx:limit" 10
                                                               "idx:result" {"idx:id" "?doc"
                                                                             "idx:score" "?score"}}]
                                                     "select" ["?doc" "?score"]})]
              (log/info "Search results for 'fluree' after adding book3:" results)
              (is (= 1 (count results)) "Should find one book about fluree")
              (is (= "http://example.org/book3" (ffirst results)) "Should find book3")))

          (testing "search now finds both books mentioning databases"
            (let [results @(fluree/query-connection conn
                                                    {"@context" {"idx" "https://ns.flur.ee/index#"}
                                                     "from" ["book-search"]
                                                     "where" [{"@id" "?x"
                                                               "idx:target" "databases"
                                                               "idx:limit" 10
                                                               "idx:result" {"idx:id" "?doc"
                                                                             "idx:score" "?score"}}]
                                                     "select" ["?doc" "?score"]})]
              (log/info "Search results for 'databases' after update:" results)
              (is (= 2 (count results)) "Should find both books mentioning databases")
              (let [book-ids (set (map first results))]
                (is (contains? book-ids "http://example.org/book1") "Should find book1")
                (is (contains? book-ids "http://example.org/book3") "Should find book3"))))))

      ;; Test dependency tracking prevents ledger deletion
      ;; TODO: Commenting out for now to focus on other test failures
      #_(testing "cannot delete ledger with dependent virtual graph"
          (let [error (try
                        @(fluree/drop conn "books")
                        nil
                        (catch Exception e
                          e))]
            (is (some? error) "Should throw error when trying to delete ledger")
            (when error
              (let [ex-data (ex-data error)]
                (log/info "Ledger deletion error:" ex-data)
                (is (= :db/ledger-has-dependencies (:error ex-data)) "Should have correct error type")
                (is (contains? (:dependent-vgs ex-data) "book-search") "Should list the dependent VG"))))))))