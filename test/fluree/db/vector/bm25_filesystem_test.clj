(ns fluree.db.vector.bm25-filesystem-test
  (:require [babashka.fs :as fs]
            [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.util.json :as json]))

(deftest ^:integration bm25-filesystem-test
  (testing "BM25 virtual graph creation with filesystem verification"
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
                                             "ex:content" "Fluree is a semantic graph database"}
                                            {"@id" "ex:article2"
                                             "@type" "ex:Article"
                                             "ex:title" "Advanced Queries"
                                             "ex:content" "Learn about complex query patterns"}]})

            ;; Create BM25 virtual graph
            vg-result @(fluree/create-virtual-graph
                        conn
                        {:name "article-search"
                         :type :bm25
                         :config {:ledgers ["articles"]
                                  :query {"@context" {"ex" "http://example.org/"}
                                          "where" [{"@id" "?x"
                                                    "@type" "ex:Article"}]
                                          "select" {"?x" ["@id" "ex:title" "ex:content"]}}}})

            ;; Allow time for BM25 index building and async operations to complete
            _ (Thread/sleep 5000)

            ;; Force a query to trigger VG loading
            _ (println "\nForcing VG load by directly querying...")
            vg-query-result (try
                              @(fluree/query
                                "##article-search"
                                {:context {"idx" "https://ns.flur.ee/index#"
                                           "ex" "http://example.org/"}
                                 :where [{"@id" "?x"
                                          "idx:target" "fluree"
                                          "idx:limit" 10
                                          "idx:result" {"idx:id" "?article"
                                                        "idx:score" "?score"}}]
                                 :select ["?article" "?score"]})
                              (catch Exception e
                                (println "Direct VG query error:" (.getMessage e))
                                nil))
            _ (println "Direct VG query result:" vg-query-result)

            ;; Also try through connection
            _ (println "\nTrying through connection API...")
            conn-vg-result (try
                             @(fluree/load conn "##article-search")
                             (catch Exception e
                               (println "Load VG error:" (.getMessage e))
                               nil))
            _ (println "Load VG result:" conn-vg-result)
            _ (Thread/sleep 2000)]

        (testing "virtual graph creation returns success"
          (is (= "article-search" vg-result)))

        (testing "nameservice record is created on disk"
          (let [ns-file (fs/file storage-path "ns@v1" "article-search.json")]
            (is (fs/exists? ns-file) "Nameservice file should exist")

            (when (fs/exists? ns-file)
              (let [ns-content (json/parse (slurp ns-file) false)]
                (is (= "article-search" (get ns-content "@id")) "VG name should match")
                (is (some #{"f:VirtualGraphDatabase"} (get ns-content "@type")) "Should have VG type")
                (is (some #{"fidx:BM25"} (get ns-content "@type")) "Should have BM25 type")
                (is (get-in ns-content ["fidx:config" "@value"]) "Should have config")
                (is (get ns-content "f:dependencies") "Should have dependencies")

                (println "Nameservice record:")
                (println (json/stringify ns-content))))))

        (testing "BM25 index data is written to disk"
          ;; Check for BM25 index directory structure
          (let [expected-bm25-dir (fs/file storage-path "virtual-graphs" "article-search" "bm25")
                vg-base-dir (fs/file storage-path "virtual-graphs" "article-search")]
            (println "Looking for BM25 index directory:" (str expected-bm25-dir))
            (println "VG base directory exists:" (fs/exists? vg-base-dir))
            (println "BM25 directory exists:" (fs/exists? expected-bm25-dir))

            (when (fs/exists? vg-base-dir)
              (println "VG base directory contents:" (mapv str (fs/list-dir vg-base-dir))))
            (when (fs/exists? expected-bm25-dir)
              (println "BM25 directory contents:" (mapv str (fs/list-dir expected-bm25-dir))))

            ;; The BM25 index should be written to virtual-graphs/{name}/bm25/
            (is (fs/exists? expected-bm25-dir) "BM25 index directory should exist at expected path")))

        (testing "filesystem structure verification"
          (println "\\nComplete filesystem structure:")
          (letfn [(print-tree [dir level]
                    (when (fs/exists? dir)
                      (doseq [item (fs/list-dir dir)]
                        (println (str (apply str (repeat level "  ")) "- " (fs/file-name item)))
                        (when (fs/directory? item)
                          (print-tree item (inc level))))))]
            (print-tree (fs/file storage-path) 0))

          ;; Basic filesystem assertions
          (is (fs/exists? (fs/file storage-path)) "Storage directory should exist")
          (is (fs/exists? (fs/file storage-path "ns@v1")) "Nameservice directory should exist"))))))