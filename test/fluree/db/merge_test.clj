(ns fluree.db.merge-test
  (:require [babashka.fs :as fs]
            [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]))

(deftest ^:integration fast-forward-merge-test
  (testing "Fast-forward merge when target is behind source"
    (let [conn @(fluree/connect-memory {})]
      (try
        ;; Create ledger and add initial data to main
        @(fluree/create conn "merge-test" {})
        @(fluree/insert! conn "merge-test:main"
                         {"@context" {"ex" "http://example.org/"}
                          "@graph" [{"@id" "ex:alice"
                                     "@type" "Person"
                                     "ex:name" "Alice"}]})

        ;; Create feature branch from main
        @(fluree/create-branch! conn "merge-test:feature" "merge-test:main")

        ;; Add data only to feature branch
        @(fluree/insert! conn "merge-test:feature"
                         {"@context" {"ex" "http://example.org/"}
                          "@graph" [{"@id" "ex:bob"
                                     "@type" "Person"
                                     "ex:name" "Bob"}]})

        ;; Check divergence
        (let [divergence @(fluree/branch-divergence conn "merge-test:feature" "merge-test:main")]
          (is (:can-fast-forward divergence) "Should be able to fast-forward")
          (is (= :branch1-to-branch2 (:fast-forward-direction divergence))))

        ;; Merge feature into main (should be fast-forward)
        (let [merge-result @(fluree/rebase! conn "merge-test:feature" "merge-test:main")]
          (println "Fast-forward merge result:" merge-result)
          (is (= :success (:status merge-result)) "Merge should succeed")
          (is (= "fast-forward" (:strategy merge-result)) "Should be a fast-forward merge")
          (is (= "merge-test:feature" (:from merge-result)))
          (is (= "merge-test:main" (:to merge-result))))

        ;; Verify main now has both Alice and Bob
        (let [main-db @(fluree/load conn "merge-test:main")
              results @(fluree/query main-db {"@context" {"ex" "http://example.org/"}
                                              "select" "?name"
                                              "where" {"@id" "?id"
                                                       "ex:name" "?name"}})]
          (is (= #{"Alice" "Bob"} (set results)) "Main should have both Alice and Bob after merge"))

        (finally
          @(fluree/disconnect conn))))))

(deftest ^:integration squash-merge-test
  (testing "Squash merge with multiple changes"
    (let [conn @(fluree/connect-memory {})]
      (try
        ;; Create ledger with initial data
        @(fluree/create conn "flatten-test" {})
        @(fluree/insert! conn "flatten-test:main"
                         {"@context" {"ex" "http://example.org/"}
                          "@graph" [{"@id" "ex:alice"
                                     "@type" "Person"
                                     "ex:name" "Alice"
                                     "ex:age" 30}]})

        ;; Create feature branch
        @(fluree/create-branch! conn "flatten-test:feature" "flatten-test:main")

        ;; Make changes in both branches
        ;; Main: Update Alice's age
        @(fluree/update! conn "flatten-test:main"
                         {"@context" {"ex" "http://example.org/"}
                          "where" {"@id" "ex:alice"
                                   "ex:age" "?age"}
                          "delete" {"@id" "ex:alice"
                                    "ex:age" "?age"}
                          "insert" {"@id" "ex:alice"
                                    "ex:age" 31}})

        ;; Feature: Add Bob and update Alice's name (different property)
        @(fluree/update! conn "flatten-test:feature"
                         {"@context" {"ex" "http://example.org/"}
                          "where" {"@id" "ex:alice"
                                   "ex:name" "?name"}
                          "delete" {"@id" "ex:alice"
                                    "ex:name" "?name"}
                          "insert" {"@id" "ex:alice"
                                    "ex:name" "Alice Smith"}})

        @(fluree/insert! conn "flatten-test:feature"
                         {"@context" {"ex" "http://example.org/"}
                          "@graph" [{"@id" "ex:bob"
                                     "@type" "Person"
                                     "ex:name" "Bob"
                                     "ex:age" 25}]})

        ;; Multiple updates to same data in feature branch (to test flattening)
        @(fluree/update! conn "flatten-test:feature"
                         {"@context" {"ex" "http://example.org/"}
                          "where" {"@id" "ex:bob"
                                   "ex:age" "?age"}
                          "delete" {"@id" "ex:bob"
                                    "ex:age" "?age"}
                          "insert" {"@id" "ex:bob"
                                    "ex:age" 26}})

        @(fluree/update! conn "flatten-test:feature"
                         {"@context" {"ex" "http://example.org/"}
                          "where" {"@id" "ex:bob"
                                   "ex:age" "?age"}
                          "delete" {"@id" "ex:bob"
                                    "ex:age" "?age"}
                          "insert" {"@id" "ex:bob"
                                    "ex:age" 27}})

        ;; Check divergence
        (let [divergence @(fluree/branch-divergence conn "flatten-test:feature" "flatten-test:main")]
          (is (not (:can-fast-forward divergence)) "Should not be able to fast-forward"))

        ;; Merge with squash strategy
        (let [merge-result @(fluree/rebase! conn "flatten-test:feature" "flatten-test:main"
                                            {:squash? true
                                             :message "Squash merge feature into main"})]
          (is (= :success (:status merge-result)) "Merge should succeed")
          (is (= "squash" (:strategy merge-result)) "Should be a squash merge"))

        ;; Verify final state in main
        (let [main-db @(fluree/load conn "flatten-test:main")
              alice-result @(fluree/query main-db {"@context" {"ex" "http://example.org/"}
                                                   "select" ["?name" "?age"]
                                                   "where" {"@id" "ex:alice"
                                                            "ex:name" "?name"
                                                            "ex:age" "?age"}})
              bob-result @(fluree/query main-db {"@context" {"ex" "http://example.org/"}
                                                 "select" ["?name" "?age"]
                                                 "where" {"@id" "ex:bob"
                                                          "ex:name" "?name"
                                                          "ex:age" "?age"}})]
          ;; Alice should have name from feature branch but age from main
          (is (= [["Alice Smith" 31]] alice-result) "Alice should have merged changes")
          ;; Bob should have final age (27) after multiple updates
          (is (= [["Bob" 27]] bob-result) "Bob should have final flattened state"))

        (finally
          @(fluree/disconnect conn))))))

(deftest ^:integration file-squash-merge-test
  (testing "Squash merge on file-backed storage"
    (fs/with-temp-dir [tmp-dir {:prefix "fluree-squash-test-"}]
      (let [storage-path (str tmp-dir)
            conn @(fluree/connect-file {:storage-path storage-path})]
        (try
          ;; Create ledger with initial data
          @(fluree/create conn "flatten-file-test" {})
          @(fluree/insert! conn "flatten-file-test:main"
                           {"@context" {"ex" "http://example.org/"}
                            "@graph" [{"@id" "ex:alice"
                                       "@type" "Person"
                                       "ex:name" "Alice"
                                       "ex:age" 30}]})

          ;; Create feature branch
          @(fluree/create-branch! conn "flatten-file-test:feature" "flatten-file-test:main")

          ;; Make changes in both branches
          ;; Main: Update Alice's age
          @(fluree/update! conn "flatten-file-test:main"
                           {"@context" {"ex" "http://example.org/"}
                            "where" {"@id" "ex:alice"
                                     "ex:age" "?age"}
                            "delete" {"@id" "ex:alice"
                                      "ex:age" "?age"}
                            "insert" {"@id" "ex:alice"
                                      "ex:age" 31}})

          ;; Feature: Add Bob and update Alice's name (different property)
          @(fluree/update! conn "flatten-file-test:feature"
                           {"@context" {"ex" "http://example.org/"}
                            "where" {"@id" "ex:alice"
                                     "ex:name" "?name"}
                            "delete" {"@id" "ex:alice"
                                      "ex:name" "?name"}
                            "insert" {"@id" "ex:alice"
                                      "ex:name" "Alice Smith"}})

          @(fluree/insert! conn "flatten-file-test:feature"
                           {"@context" {"ex" "http://example.org/"}
                            "@graph" [{"@id" "ex:bob"
                                       "@type" "Person"
                                       "ex:name" "Bob"
                                       "ex:age" 25}]})

          ;; Multiple updates to same data in feature branch (to test flattening)
          @(fluree/update! conn "flatten-file-test:feature"
                           {"@context" {"ex" "http://example.org/"}
                            "where" {"@id" "ex:bob"
                                     "ex:age" "?age"}
                            "delete" {"@id" "ex:bob"
                                      "ex:age" "?age"}
                            "insert" {"@id" "ex:bob"
                                      "ex:age" 26}})

          @(fluree/update! conn "flatten-file-test:feature"
                           {"@context" {"ex" "http://example.org/"}
                            "where" {"@id" "ex:bob"
                                     "ex:age" "?age"}
                            "delete" {"@id" "ex:bob"
                                      "ex:age" "?age"}
                            "insert" {"@id" "ex:bob"
                                      "ex:age" 27}})

          ;; Check divergence
          (let [divergence @(fluree/branch-divergence conn "flatten-file-test:feature" "flatten-file-test:main")]
            (is (not (:can-fast-forward divergence)) "Should not be able to fast-forward"))

          ;; Merge with squash strategy
          (let [merge-result @(fluree/rebase! conn "flatten-file-test:feature" "flatten-file-test:main"
                                              {:squash? true
                                               :message "Squash merge feature into main"})]
            (is (= :success (:status merge-result)) "Merge should succeed")
            (is (= "squash" (:strategy merge-result)) "Should be a squash merge"))

          ;; Verify final state in main
          (let [main-db @(fluree/load conn "flatten-file-test:main")
                alice-result @(fluree/query main-db {"@context" {"ex" "http://example.org/"}
                                                     "select" ["?name" "?age"]
                                                     "where" {"@id" "ex:alice"
                                                              "ex:name" "?name"
                                                              "ex:age" "?age"}})
                bob-result @(fluree/query main-db {"@context" {"ex" "http://example.org/"}
                                                   "select" ["?name" "?age"]
                                                   "where" {"@id" "ex:bob"
                                                            "ex:name" "?name"
                                                            "ex:age" "?age"}})]
            (is (= [["Alice Smith" 31]] alice-result) "Alice should have merged changes")
            (is (= [["Bob" 27]] bob-result) "Bob should have final flattened state"))

          (finally
            @(fluree/disconnect conn)))))))

(deftest ^:integration merge-conflict-test
  (testing "Merge conflict detection when same data modified in both branches"
    (let [conn @(fluree/connect-memory {})]
      (try
        ;; Create ledger with initial data
        @(fluree/create conn "conflict-test" {})
        @(fluree/insert! conn "conflict-test:main"
                         {"@context" {"ex" "http://example.org/"}
                          "@graph" [{"@id" "ex:alice"
                                     "@type" "Person"
                                     "ex:name" "Alice"
                                     "ex:age" 30}]})

        ;; Create feature branch
        @(fluree/create-branch! conn "conflict-test:feature" "conflict-test:main")

        ;; Make conflicting changes - both branches modify Alice's name
        @(fluree/update! conn "conflict-test:main"
                         {"@context" {"ex" "http://example.org/"}
                          "where" {"@id" "ex:alice"
                                   "ex:name" "?name"}
                          "delete" {"@id" "ex:alice"
                                    "ex:name" "?name"}
                          "insert" {"@id" "ex:alice"
                                    "ex:name" "Alice Jones"}})

        @(fluree/update! conn "conflict-test:feature"
                         {"@context" {"ex" "http://example.org/"}
                          "where" {"@id" "ex:alice"
                                   "ex:name" "?name"}
                          "delete" {"@id" "ex:alice"
                                    "ex:name" "?name"}
                          "insert" {"@id" "ex:alice"
                                    "ex:name" "Alice Smith"}})

        ;; Attempt merge - should detect conflict
        (let [merge-result @(fluree/rebase! conn "conflict-test:feature" "conflict-test:main"
                                            {:squash? true})]
          (is (= :conflict (:status merge-result)) "Should have conflict status")
          (is (= :db/rebase-conflict (:error merge-result)) "Should have rebase conflict error")
          (when (map? merge-result)
            (is (contains? merge-result :commits) "Should include commit details")
            (is (seq (get-in merge-result [:commits :conflicts])) "Should have conflicts")
            (is (= "conflict-test:feature" (:from merge-result)))
            (is (= "conflict-test:main" (:to merge-result)))))

        (finally
          @(fluree/disconnect conn))))))

(deftest ^:integration merge-strategy-test
  (testing "Different merge strategies"
    (let [conn @(fluree/connect-memory {})]
      (try
        ;; Setup initial state
        @(fluree/create conn "strategy-test" {})
        @(fluree/insert! conn "strategy-test:main"
                         {"@context" {"ex" "http://example.org/"}
                          "@graph" [{"@id" "ex:alice"
                                     "@type" "Person"
                                     "ex:name" "Alice"}]})

        ;; Test forced fast-forward strategy when not possible
        (testing "Fast-forward strategy when not possible"
          @(fluree/create-branch! conn "strategy-test:ff-test" "strategy-test:main")

          ;; Add changes to both branches
          @(fluree/insert! conn "strategy-test:main"
                           {"@context" {"ex" "http://example.org/"}
                            "@graph" [{"@id" "ex:charlie"
                                       "@type" "Person"
                                       "ex:name" "Charlie"}]})

          @(fluree/insert! conn "strategy-test:ff-test"
                           {"@context" {"ex" "http://example.org/"}
                            "@graph" [{"@id" "ex:david"
                                       "@type" "Person"
                                       "ex:name" "David"}]})

          ;; Try fast-forward only - should fail since FF not possible
          (let [merge-result @(fluree/rebase! conn "strategy-test:ff-test" "strategy-test:main"
                                              {:ff :only})]
            ;; Since fast-forward is not possible, it should return error
            (is (= :error (:status merge-result)) "Should fail when FF not possible")
            (is (= :db/cannot-fast-forward (:error merge-result)) "Should have cannot-fast-forward error")))

        ;; Test no-ff strategy
        (testing "No fast-forward strategy"
          @(fluree/create-branch! conn "strategy-test:no-ff" "strategy-test:main")

          ;; Only add to feature branch (normally would allow FF)
          @(fluree/insert! conn "strategy-test:no-ff"
                           {"@context" {"ex" "http://example.org/"}
                            "@graph" [{"@id" "ex:eve"
                                       "@type" "Person"
                                       "ex:name" "Eve"}]})

          ;; Force non-fast-forward merge (use squash instead of FF)
          (let [merge-result @(fluree/rebase! conn "strategy-test:no-ff" "strategy-test:main"
                                              {:ff :never
                                               :squash? true
                                               :message "Force merge commit"})]
            (is (= :success (:status merge-result)) "Should succeed")
            (is (= "squash" (:strategy merge-result)) "Should use squash instead of FF")))

        (finally
          @(fluree/disconnect conn))))))

(deftest ^:integration complex-merge-scenario-test
  (testing "Complex merge scenario with multiple branches"
    (let [conn @(fluree/connect-memory {})]
      (try
        ;; Create initial repository state
        @(fluree/create conn "complex-test" {})
        @(fluree/insert! conn "complex-test:main"
                         {"@context" {"ex" "http://example.org/"
                                      "foaf" "http://xmlns.com/foaf/0.1/"}
                          "@graph" [{"@id" "ex:project"
                                     "@type" "foaf:Project"
                                     "foaf:name" "Main Project"
                                     "ex:version" "1.0.0"}]})

        ;; Create feature-a branch
        @(fluree/create-branch! conn "complex-test:feature-a" "complex-test:main")
        @(fluree/insert! conn "complex-test:feature-a"
                         {"@context" {"ex" "http://example.org/"}
                          "@graph" [{"@id" "ex:feature-a"
                                     "@type" "Feature"
                                     "ex:name" "Feature A"
                                     "ex:status" "development"}]})

        ;; Create feature-b branch from main
        @(fluree/create-branch! conn "complex-test:feature-b" "complex-test:main")
        @(fluree/insert! conn "complex-test:feature-b"
                         {"@context" {"ex" "http://example.org/"}
                          "@graph" [{"@id" "ex:feature-b"
                                     "@type" "Feature"
                                     "ex:name" "Feature B"
                                     "ex:priority" "high"}]})

        ;; Merge feature-a into main
        (let [merge-a @(fluree/rebase! conn "complex-test:feature-a" "complex-test:main")]
          (is (= :success (:status merge-a)) "First merge should succeed")
          (is (= "fast-forward" (:strategy merge-a)) "First merge should be fast-forward"))

        ;; Now merge feature-b into main (should handle divergence properly)
        (let [merge-b @(fluree/rebase! conn "complex-test:feature-b" "complex-test:main"
                                       {:squash? true})]
          (is (= :success (:status merge-b)) "Second merge should succeed")
          (is (= "squash" (:strategy merge-b)) "Second merge should be squash due to divergence"))

        ;; Verify final state has all features
        (let [main-db @(fluree/load conn "complex-test:main")
              features @(fluree/query main-db {"@context" {"ex" "http://example.org/"}
                                               "select" "?name"
                                               "where" {"@type" "Feature"
                                                        "ex:name" "?name"}})]
          (is (= #{"Feature A" "Feature B"} (set features)) "Main should have both features"))

        (finally
          @(fluree/disconnect conn))))))