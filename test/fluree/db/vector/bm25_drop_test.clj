(ns fluree.db.vector.bm25-drop-test
  "Test that virtual graphs can be properly dropped"
  (:require [clojure.core.async :refer [<!!]]
            [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.virtual-graph :as vg]))

(deftest ^:integration drop-virtual-graph-test
  (testing "Drop virtual graph removes all artifacts"
    (let [conn    (test-utils/create-conn)
          _ledger @(fluree/create conn "movies")]

      ;; Add some test data
      @(fluree/insert! conn "movies"
                       {"@context" {"ex" "http://example.org/"}
                        "@graph" [{"@id" "ex:movie1"
                                   "@type" "ex:Movie"
                                   "ex:title" "The Matrix"
                                   "ex:year" 1999
                                   "ex:description" "A computer hacker learns about the true nature of reality"}
                                  {"@id" "ex:movie2"
                                   "@type" "ex:Movie"
                                   "ex:title" "Inception"
                                   "ex:year" 2010
                                   "ex:description" "A thief who steals corporate secrets through dream-sharing technology"}]})

      ;; Create a BM25 virtual graph
      (let [vg-obj @(fluree/create-virtual-graph
                     conn
                     {:name "movie-search"
                      :type :bm25
                      :config {:ledgers ["movies"]
                               :query {"@context" {"ex" "http://example.org/"}
                                       "where" [{"@id" "?x"
                                                 "@type" "ex:Movie"}]
                                       "select" {"?x" ["@id" "ex:title" "ex:description"]}}}})]

        (testing "VG is created successfully"
          (is (some? vg-obj))
          ;; VG names follow ledger convention - normalized with :main branch
          (is (= "movie-search:main" (:vg-name vg-obj))))

        ;; Wait for indexing to complete
        (<!! (vg/sync vg-obj nil))

        ;; Verify we can query it
        (testing "VG search works before deletion"
          (let [results @(fluree/query-connection conn
                                                  {"@context" {"idx" "https://ns.flur.ee/index#"}
                                                   "from" ["movie-search"]
                                                   "where" [{"@id" "?x"
                                                             "idx:target" "matrix"
                                                             "idx:limit" 10
                                                             "idx:result" {"idx:id" "?doc"
                                                                           "idx:score" "?score"}}]
                                                   "select" ["?doc" "?score"]})]
            (is (= 1 (count results)) "Should find one movie about matrix")))

        ;; Drop the virtual graph
        (testing "dropping virtual graph"
          (let [drop-result @(fluree/drop-virtual-graph conn "movie-search")]
            (is (= :dropped drop-result))))

        ;; Verify we can no longer query it
        (testing "VG is not accessible after deletion"
          (let [result @(fluree/query-connection conn
                                                 {"@context" {"idx" "https://ns.flur.ee/index#"}
                                                  "from" ["movie-search"]
                                                  "where" [{"@id" "?x"
                                                            "idx:target" "matrix"
                                                            "idx:limit" 10
                                                            "idx:result" {"idx:id" "?doc"}}]
                                                  "select" ["?doc"]})]
            (is (instance? Exception result) "Should return an exception when querying deleted VG")
            (when (instance? Exception result)
              (let [data (ex-data result)]
                (is (integer? (:status data)) "Error should have a numeric status")
                (is (some? (:error data)) "Error should include a :error code")))))

        ;; Verify we can recreate a VG with the same name
        (testing "can recreate VG with same name after deletion"
          (let [new-vg @(fluree/create-virtual-graph
                         conn
                         {:name "movie-search"
                          :type :bm25
                          :config {:ledgers ["movies"]
                                   :query {"@context" {"ex" "http://example.org/"}
                                           "where" [{"@id" "?x"
                                                     "@type" "ex:Movie"}]
                                           "select" {"?x" ["@id" "ex:title"]}}}})]
            (is (some? new-vg))
            (is (= "movie-search:main" (:vg-name new-vg)))))))))

;; TODO: Dependency protection for ledger deletion not yet implemented.
;; Uncomment when drop-ledger checks for dependent VGs before deletion.
#_(deftest ^:integration drop-vg-with-dependencies-test
    (testing "Ledger cannot be dropped when VG depends on it"
      (let [conn    (test-utils/create-conn)
            _ledger @(fluree/create conn "books")]

        ;; Add test data
        @(fluree/insert! conn "books"
                         {"@context" {"ex" "http://example.org/"}
                          "@graph" [{"@id" "ex:book1"
                                     "@type" "ex:Book"
                                     "ex:title" "Fluree Guide"
                                     "ex:content" "Learn about graph databases"}]})

        ;; Create VG that depends on books ledger
        (let [vg @(fluree/create-virtual-graph
                   conn
                   {:name "book-index"
                    :type :bm25
                    :config {:ledgers ["books"]
                             :query {"@context" {"ex" "http://example.org/"}
                                     "where" [{"@id" "?x"
                                               "@type" "ex:Book"}]
                                     "select" {"?x" ["@id" "ex:title" "ex:content"]}}}})]
          ;; Wait for VG to be initialized
          (<!! (vg/sync vg nil)))

        ;; Try to drop the ledger while VG still exists
        (testing "cannot drop ledger with dependent VG"
          (let [result @(fluree/drop conn "books")]
            (is (instance? Exception result) "Should return an exception")
            (when (instance? Exception result)
              (is (re-find #"Cannot delete ledger.*has dependent virtual graphs" (ex-message result))
                  (str "Error message doesn't match. Got: " (ex-message result))))))

        ;; Drop the VG first
        (testing "can drop ledger after dropping VG"
          (is (= :dropped @(fluree/drop-virtual-graph conn "book-index")))
          ;; Now we should be able to drop the ledger
          (is (= :dropped @(fluree/drop conn "books")))))))