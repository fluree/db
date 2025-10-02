(ns fluree.db.merge-test
  (:require [babashka.fs :as fs]
            [clojure.string :as str]
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
        (let [merge-result @(fluree/merge! conn "merge-test:feature" "merge-test:main")]
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
        (let [merge-result @(fluree/merge! conn "flatten-test:feature" "flatten-test:main"
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

(deftest ^:integration squash-cancellation-test
  (testing "Squash merge cancels out assert/retract pairs"
    (let [conn @(fluree/connect-memory {})]
      (try
        ;; Create ledger with initial data
        @(fluree/create conn "cancel-test" {})
        @(fluree/insert! conn "cancel-test:main"
                         {"@context" {"ex" "http://example.org/"}
                          "@graph" [{"@id" "ex:alice"
                                     "@type" "Person"
                                     "ex:name" "Alice"}]})

        ;; Create feature branch
        @(fluree/create-branch! conn "cancel-test:feature" "cancel-test:main")

        ;; In feature branch:
        ;; Commit 1: Add skills to Alice
        @(fluree/insert! conn "cancel-test:feature"
                         {"@context" {"ex" "http://example.org/"}
                          "@graph" [{"@id" "ex:alice"
                                     "ex:skills" ["Java" "Python" "Rust"]}]})

        ;; Commit 2: Remove some skills (including Rust)
        @(fluree/update! conn "cancel-test:feature"
                         {"@context" {"ex" "http://example.org/"}
                          "delete" {"@id" "ex:alice"
                                    "ex:skills" ["Java" "Rust"]}})

        ;; Commit 3: Re-add Rust (and JavaScript)
        @(fluree/insert! conn "cancel-test:feature"
                         {"@context" {"ex" "http://example.org/"}
                          "@graph" [{"@id" "ex:alice"
                                     "ex:skills" ["Rust" "JavaScript"]}]})

        ;; Squash merge feature into main
        (let [merge-result @(fluree/merge! conn "cancel-test:feature" "cancel-test:main"
                                           {:squash? true})]
          (is (= :success (:status merge-result)) "Squash merge should succeed"))

        ;; Verify final state in main
        ;; Java: asserted then retracted = cancelled out (should not exist)
        ;; Rust: asserted, retracted, then asserted again = net assertion (should exist)
        ;; Python: only asserted = should exist
        ;; JavaScript: only asserted = should exist
        (let [main-db @(fluree/load conn "cancel-test:main")
              result @(fluree/query main-db {"@context" {"ex" "http://example.org/"}
                                             "select" "?skill"
                                             "where" {"@id" "ex:alice"
                                                      "ex:skills" "?skill"}})]
          (is (= #{"Python" "Rust" "JavaScript"} (set result))
              "Should have Python, Rust, and JavaScript but not Java (cancelled out)"))

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
          (let [merge-result @(fluree/merge! conn "flatten-file-test:feature" "flatten-file-test:main"
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
        (let [merge-result @(fluree/merge! conn "conflict-test:feature" "conflict-test:main"
                                           {:squash? true})]
          (is (= :conflict (:status merge-result)) "Should have conflict status")
          (is (= :db/merge-conflict (:error merge-result)) "Should have merge conflict error")
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
          (let [merge-result @(fluree/merge! conn "strategy-test:ff-test" "strategy-test:main"
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
          (let [merge-result @(fluree/merge! conn "strategy-test:no-ff" "strategy-test:main"
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
        (let [merge-a @(fluree/merge! conn "complex-test:feature-a" "complex-test:main")]
          (is (= :success (:status merge-a)) "First merge should succeed")
          (is (= "fast-forward" (:strategy merge-a)) "First merge should be fast-forward"))

        ;; Now merge feature-b into main (should handle divergence properly)
        (let [merge-b @(fluree/merge! conn "complex-test:feature-b" "complex-test:main"
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

(deftest ^:integration namespace-collision-test
  (testing "Rebase handles namespace collisions correctly"
    (let [conn @(fluree/connect-memory {})]
      (try
        ;; Create initial ledger with base data
        @(fluree/create conn "ns-test" {})
        @(fluree/insert! conn "ns-test:main"
                         {"@context" {"ex" "http://example.org/"
                                      "schema" "http://schema.org/"}
                          "@graph" [{"@id" "ex:alice"
                                     "@type" "schema:Person"
                                     "schema:name" "Alice"}]})

        ;; Create two branches
        @(fluree/create-branch! conn "ns-test:feature1" "ns-test:main")
        @(fluree/create-branch! conn "ns-test:feature2" "ns-test:main")

        ;; In feature1, add a new namespace and use it
        @(fluree/insert! conn "ns-test:feature1"
                         {"@context" {"ex" "http://example.org/"
                                      "custom" "http://custom.org/"}
                          "@graph" [{"@id" "ex:bob"
                                     "@type" "custom:Employee"
                                     "custom:employeeId" "12345"}]})

        ;; In feature2, add a different namespace and use it
        @(fluree/insert! conn "ns-test:feature2"
                         {"@context" {"ex" "http://example.org/"
                                      "internal" "http://internal.org/"}
                          "@graph" [{"@id" "ex:charlie"
                                     "@type" "internal:Manager"
                                     "internal:managerId" "67890"}]})

        ;; Merge feature1 into main
        (let [merge1 @(fluree/merge! conn "ns-test:feature1" "ns-test:main")]
          (is (= :success (:status merge1)) "First merge should succeed"))

        ;; Now merge feature2 into main - this will need to handle namespace collision
        ;; The internal:Manager namespace might get a different integer ID during merge
        (let [merge2 @(fluree/merge! conn "ns-test:feature2" "ns-test:main"
                                     {:squash? true})]
          (is (= :success (:status merge2)) "Second merge should succeed despite namespace differences"))

        ;; Verify final state has all data with correct namespaces
        (let [main-db @(fluree/load conn "ns-test:main")
              alice @(fluree/query main-db {"@context" {"ex" "http://example.org/"
                                                        "schema" "http://schema.org/"}
                                            "select" "?name"
                                            "where" {"@id" "ex:alice"
                                                     "schema:name" "?name"}})
              bob @(fluree/query main-db {"@context" {"ex" "http://example.org/"
                                                      "custom" "http://custom.org/"}
                                          "select" "?id"
                                          "where" {"@id" "ex:bob"
                                                   "custom:employeeId" "?id"}})
              charlie @(fluree/query main-db {"@context" {"ex" "http://example.org/"
                                                          "internal" "http://internal.org/"}
                                              "select" "?id"
                                              "where" {"@id" "ex:charlie"
                                                       "internal:managerId" "?id"}})]
          (is (= ["Alice"] alice) "Alice should exist with correct name")
          (is (= ["12345"] bob) "Bob should exist with correct employee ID")
          (is (= ["67890"] charlie) "Charlie should exist with correct manager ID"))

        (finally
          @(fluree/disconnect conn))))))

(deftest ^:integration reset-branch-test
  (testing "Reset branch to previous state"
    (let [conn @(fluree/connect-memory {})]
      (try
        ;; Create ledger with initial data
        @(fluree/create conn "reset-test" {})

        ;; First commit - add Alice
        @(fluree/insert! conn "reset-test:main"
                         {"@context" {"ex" "http://example.org/"}
                          "@graph" [{"@id" "ex:alice"
                                     "@type" "Person"
                                     "ex:name" "Alice"
                                     "ex:age" 30}]})

        (let [db-after-alice @(fluree/load conn "reset-test:main")
              t-after-alice (:t db-after-alice)]

          ;; Second commit - add Bob
          @(fluree/insert! conn "reset-test:main"
                           {"@context" {"ex" "http://example.org/"}
                            "@graph" [{"@id" "ex:bob"
                                       "@type" "Person"
                                       "ex:name" "Bob"
                                       "ex:age" 25}]})

          ;; Third commit - update Alice's age
          @(fluree/update! conn "reset-test:main"
                           {"@context" {"ex" "http://example.org/"}
                            "where" {"@id" "ex:alice"
                                     "ex:age" "?age"}
                            "delete" {"@id" "ex:alice"
                                      "ex:age" "?age"}
                            "insert" {"@id" "ex:alice"
                                      "ex:age" 31}})

          ;; Verify current state has both Alice (age 31) and Bob
          (let [current-db @(fluree/load conn "reset-test:main")
                current-result @(fluree/query current-db {"@context" {"ex" "http://example.org/"}
                                                          "select" ["?name" "?age"]
                                                          "where" {"@id" "?id"
                                                                   "ex:name" "?name"
                                                                   "ex:age" "?age"}
                                                          "order-by" "?name"})]
            (is (= [["Alice" 31] ["Bob" 25]] current-result) "Current state should have Alice (31) and Bob"))

          ;; Reset to after first commit (only Alice with age 30)
          (let [reset-result @(fluree/reset-branch! conn "reset-test:main"
                                                    {:t t-after-alice}
                                                    {:message "Revert to initial state with only Alice"})]
            (is (= :success (:status reset-result)) "Reset should succeed")
            (is (= :safe (:mode reset-result)) "Should use safe mode")
            (is (string? (:new-commit reset-result)) "Should create a new commit")

            ;; Verify state after reset
            (let [reset-db @(fluree/load conn "reset-test:main")
                  reset-result @(fluree/query reset-db {"@context" {"ex" "http://example.org/"}
                                                        "select" ["?name" "?age"]
                                                        "where" {"@id" "?id"
                                                                 "ex:name" "?name"
                                                                 "ex:age" "?age"}})]
              (is (= [["Alice" 30]] reset-result) "After reset should only have Alice with original age"))))

        (finally
          @(fluree/disconnect conn))))))

(deftest ^:integration reset-branch-with-sha-test
  (testing "Reset branch using commit SHA"
    (let [conn @(fluree/connect-memory {})]
      (try
        ;; Create ledger with initial data
        @(fluree/create conn "reset-sha-test" {})

        ;; First commit - add initial data
        @(fluree/insert! conn "reset-sha-test:main"
                         {"@context" {"ex" "http://example.org/"}
                          "@graph" [{"@id" "ex:project"
                                     "@type" "Project"
                                     "ex:name" "Initial Project"
                                     "ex:status" "planning"}]})

        (let [db-after-first @(fluree/load conn "reset-sha-test:main")
              first-commit-id (get-in db-after-first [:commit :id])
              ;; Extract just the SHA part if it has a prefix
              ;; The SHA includes the 'b' prefix as part of base32 encoding
              first-sha (if (str/starts-with? first-commit-id "fluree:commit:sha256:")
                          (subs first-commit-id 21)
                          first-commit-id)]

          ;; Second commit - update status
          @(fluree/update! conn "reset-sha-test:main"
                           {"@context" {"ex" "http://example.org/"}
                            "where" {"@id" "ex:project"
                                     "ex:status" "?status"}
                            "delete" {"@id" "ex:project"
                                      "ex:status" "?status"}
                            "insert" {"@id" "ex:project"
                                      "ex:status" "in-progress"}})

          ;; Third commit - add team member
          @(fluree/insert! conn "reset-sha-test:main"
                           {"@context" {"ex" "http://example.org/"}
                            "@graph" [{"@id" "ex:alice"
                                       "@type" "TeamMember"
                                       "ex:name" "Alice"
                                       "ex:role" "developer"}]})

          ;; Verify current state
          (let [current-db @(fluree/load conn "reset-sha-test:main")
                project-status @(fluree/query current-db {"@context" {"ex" "http://example.org/"}
                                                          "select" "?status"
                                                          "where" {"@id" "ex:project"
                                                                   "ex:status" "?status"}})
                team-members @(fluree/query current-db {"@context" {"ex" "http://example.org/"}
                                                        "select" "?name"
                                                        "where" {"@type" "TeamMember"
                                                                 "ex:name" "?name"}})]
            (is (= ["in-progress"] project-status) "Project should be in-progress")
            (is (= ["Alice"] team-members) "Should have Alice as team member"))

          ;; Reset using SHA to first commit
          (let [reset-result @(fluree/reset-branch! conn "reset-sha-test:main"
                                                    {:sha first-sha}
                                                    {:message "Reset to initial commit using SHA"})]
            (is (= :success (:status reset-result)) "Reset with SHA should succeed")

            ;; Verify state after reset
            (let [reset-db @(fluree/load conn "reset-sha-test:main")
                  project-status @(fluree/query reset-db {"@context" {"ex" "http://example.org/"}
                                                          "select" "?status"
                                                          "where" {"@id" "ex:project"
                                                                   "ex:status" "?status"}})
                  team-members @(fluree/query reset-db {"@context" {"ex" "http://example.org/"}
                                                        "select" "?name"
                                                        "where" {"@type" "TeamMember"
                                                                 "ex:name" "?name"}})]
              (is (= ["planning"] project-status) "Project should be back to planning status")
              (is (empty? team-members) "Should have no team members after reset"))))

        (finally
          @(fluree/disconnect conn))))))