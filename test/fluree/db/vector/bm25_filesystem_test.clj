(ns fluree.db.vector.bm25-filesystem-test
  (:require [babashka.fs :as fs]
            [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.util.json :as json]
            [fluree.db.util.log :as log]))

(deftest ^:integration bm25-filesystem-test
  (testing "BM25 virtual graph with filesystem storage"
    (fs/with-temp-dir [temp-dir {}]
      (let [storage-path (str temp-dir)
            conn @(fluree/connect-file {:storage-path storage-path})

            ;; Create ledger and add data
            _ledger @(fluree/create conn "articles")
            _db @(fluree/insert! conn "articles"
                                 {"@context" {"ex" "http://example.org/"}
                                  "@graph" [{"@id" "ex:article1"
                                             "@type" "ex:Article"
                                             "ex:title" "Introduction to Fluree"
                                             "ex:content" "Fluree is a semantic graph database with blockchain properties"}
                                            {"@id" "ex:article2"
                                             "@type" "ex:Article"
                                             "ex:title" "Advanced Query Patterns"
                                             "ex:content" "Learn about complex query patterns in graph databases"}
                                            {"@id" "ex:article3"
                                             "@type" "ex:Article"
                                             "ex:title" "Blockchain Integration"
                                             "ex:content" "How databases integrate with blockchain technology"}]})

            ;; Create BM25 virtual graph
            vg-name @(fluree/create-virtual-graph
                      conn
                      {:name "article-search"
                       :type :bm25
                       :config {:ledgers ["articles"]
                                :query {"@context" {"ex" "http://example.org/"}
                                        "where" [{"@id" "?x"
                                                  "@type" "ex:Article"}]
                                        "select" {"?x" ["@id" "ex:title" "ex:content"]}}}})]

        (testing "virtual graph creation"
          (is (= "article-search" vg-name)))

        (testing "nameservice record persistence"
          (let [ns-file (fs/file storage-path "ns@v1" "article-search.json")]
            (is (fs/exists? ns-file) "Nameservice file should exist")

            (when (fs/exists? ns-file)
              (let [ns-content (json/parse (slurp ns-file) false)]
                (is (= "article-search" (get ns-content "@id")))
                (is (some #{"f:VirtualGraphDatabase"} (get ns-content "@type")))
                (is (some #{"fidx:BM25"} (get ns-content "@type")))))))

        ;; Allow time for BM25 index building
        (Thread/sleep 3000)

        (testing "BM25 index file creation"
          (let [vg-dir (fs/file storage-path "virtual-graphs")]
            (is (fs/exists? vg-dir) "Virtual graph directory should exist")))

        (testing "search functionality"
          (let [search-results @(fluree/query-connection
                                 conn
                                 {"@context" {"idx" "https://ns.flur.ee/index#"
                                              "ex" "http://example.org/"}
                                  "from" ["article-search"]
                                  "where" [{"@id" "?x"
                                            "idx:target" "fluree"
                                            "idx:limit" 10
                                            "idx:result" {"idx:id" "?article"
                                                          "idx:score" "?score"}}]
                                  "select" ["?article" "?score"]})]

            (log/debug "Filesystem search results:" search-results)

            (testing "search returns results"
              (is (seq search-results) "Should return search results"))

            (testing "results contain expected articles"
              (let [article-ids (set (map first search-results))]
                (is (or (contains? article-ids "ex:article1")
                        (contains? article-ids "http://example.org/article1"))
                    "Should find article1 mentioning 'Fluree'")))

            (testing "results include BM25 scores"
              (is (every? #(number? (second %)) search-results) "All results should have numeric scores"))))

        (testing "search with different terms"
          (let [blockchain-results @(fluree/query-connection
                                     conn
                                     {"@context" {"idx" "https://ns.flur.ee/index#"
                                                  "ex" "http://example.org/"}
                                      "from" ["article-search"]
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
                (is (or (contains? article-ids "ex:article3")
                        (contains? article-ids "http://example.org/article3"))
                    "Should find article3 about blockchain")))))))))