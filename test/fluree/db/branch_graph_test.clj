(ns fluree.db.branch-graph-test
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.merge :as merge]))

(deftest branch-graph-test
  (testing "Branch graph visualization"
    (let [conn @(fluree/connect-memory {})]
      (try
        ;; Create a ledger with some branches and commits
        @(fluree/create conn "graph-test" {})

        ;; Make some commits on main
        @(fluree/insert! conn "graph-test"
                         {"@context" {"test" "http://example.org/test#"}
                          "@graph" [{"@id" "test:doc1"
                                     "@type" "Document"
                                     "title" "First commit"}]})

        @(fluree/insert! conn "graph-test"
                         {"@context" {"test" "http://example.org/test#"}
                          "@graph" [{"@id" "test:doc2"
                                     "@type" "Document"
                                     "title" "Second commit"}]})

        ;; Create a feature branch
        @(fluree/create-branch! conn "graph-test:feature" "graph-test:main")

        ;; Make commits on feature branch
        @(fluree/insert! conn "graph-test:feature"
                         {"@context" {"test" "http://example.org/test#"}
                          "@graph" [{"@id" "test:doc3"
                                     "@type" "Document"
                                     "title" "Feature commit"}]})

        (testing "JSON format graph"
          (let [graph @(merge/branch-graph conn "graph-test" {:format :json
                                                              :depth 10})]
            (is (map? graph) "Graph should be a map")
            (is (contains? graph :branches) "Graph should have branches")
            (is (contains? graph :commits) "Graph should have commits")
            (is (>= (count (:branches graph)) 2) "Should have at least 2 branches")
            (is (>= (count (:commits graph)) 3) "Should have at least 3 commits")

            ;; Check branch info
            (let [branches (:branches graph)]
              (is (contains? branches "main") "Should have main branch")
              (is (contains? branches "feature") "Should have feature branch")
              (is (contains? (get branches "feature") :created-from)
                  "Feature branch should have created-from info"))))

        (testing "ASCII format graph"
          (let [graph @(merge/branch-graph conn "graph-test" {:format :ascii
                                                              :depth 5})]
            (println "\n=== ASCII Graph Output ===")
            (println graph)
            (println "=========================\n")
            (is (string? graph) "ASCII graph should be a string")
            (is (re-find #"\*" graph) "ASCII graph should contain commit markers")
            (is (re-find #"main" graph) "ASCII graph should mention main branch")
            (is (re-find #"feature" graph) "ASCII graph should mention feature branch")))

        (testing "Branch filtering"
          (let [graph @(merge/branch-graph conn "graph-test" {:format :json
                                                              :branches #{"main"}})]
            (is (= 1 (count (:branches graph))) "Should only have main branch")
            (is (contains? (:branches graph) "main") "Should have main branch")))

        (finally
          @(fluree/disconnect conn))))))
