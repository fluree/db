(ns fluree.db.branch-test
  (:require [babashka.fs :refer [with-temp-dir]]
            [clojure.test :refer [deftest is testing]]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.util :as util]))

(deftest ^:integration branch-basic-operations-test
  (testing "Basic branch operations with memory connection"
    (let [conn @(fluree/connect-memory {})]
      (try
        ;; Create ledger and add initial data
        (testing "Create ledger with initial data"
          @(fluree/create conn "test-ledger" {})
          @(fluree/insert! conn "test-ledger"
                           {"@context" {"ex" "http://example.org/"}
                            "@graph" [{"@id" "ex:alice"
                                       "@type" "Person"
                                       "ex:name" "Alice"}]}))

        ;; Query main branch
        (testing "Verify initial state in main branch"
          (let [main-db @(fluree/load conn "test-ledger")
                results @(fluree/query main-db {"@context" {"ex" "http://example.org/"}
                                                "select" "?name"
                                                "where" {"@id" "ex:alice" "ex:name" "?name"}})]
            (is (= ["Alice"] results) "Should find Alice in main branch")))

        (testing "Create feature branch from main"
          (with-redefs [fluree.db.util/current-time-iso (constantly "2024-01-01T00:00:00.00000Z")]
            (let [branch-result @(fluree/create-branch! conn "test-ledger:feature-branch" "test-ledger:main")]
              (is (= "feature-branch" (:name branch-result)))
              (is (= "main" (:source-branch branch-result)))
              (is (= "2024-01-01T00:00:00.00000Z" (:created-at branch-result)))
              (is (some? (:source-commit branch-result)))
              (is (some? (:head branch-result)))))

          (testing "Add data to feature branch"
            @(fluree/insert! conn "test-ledger:feature-branch"
                             {"@context" {"ex" "http://example.org/"}
                              "@graph" [{"@id" "ex:bob"
                                         "@type" "Person"
                                         "ex:name" "Bob"}]})

            (testing "Verify branch isolation"
              ;; Main branch should only have Alice
              (let [main-db @(fluree/load conn "test-ledger:main")
                    main-results @(fluree/query main-db {"@context" {"ex" "http://example.org/"}
                                                         "select" "?name"
                                                         "where" {"@id" "?id" "ex:name" "?name"}})]
                (is (= ["Alice"] main-results) "Main branch should only have Alice"))

              ;; Feature branch should have both Alice and Bob
              (let [feature-db @(fluree/load conn "test-ledger:feature-branch")
                    feature-results @(fluree/query feature-db {"@context" {"ex" "http://example.org/"}
                                                               "select" "?name"
                                                               "where" {"@id" "?id" "ex:name" "?name"}})]
                (is (= ["Alice" "Bob"] feature-results)
                    "Feature branch should have both Alice and Bob"))))

          (testing "Branch info and metadata"
            (let [main-info @(fluree/branch-info conn "test-ledger:main")
                  feature-info @(fluree/branch-info conn "test-ledger:feature-branch")]
              (is (= "main" (:name main-info)))
              (is (some? feature-info) "feature-info should not be nil")
              (when feature-info
                (is (= "feature-branch" (:name feature-info)))
                (is (some? (:head feature-info)))
                (is (some? (:created-at feature-info)))
                (is (= "main" (:source-branch feature-info))))))

          (testing "Branch deletion"
            @(fluree/delete-branch! conn "test-ledger:feature-branch")
            ;; Verify branch is deleted by attempting to load it
            (let [result @(fluree/load conn "test-ledger:feature-branch")]
              (is (util/exception? result) "Should return an exception when loading deleted branch")
              (is (= :db/unknown-ledger (:error (ex-data result))) "Should have correct error code")
              (is (re-find #"failed due to failed address lookup" (ex-message result))
                  "Error message should indicate address lookup failed")))

          (testing "Cannot delete main branch"
            (let [result @(fluree/delete-branch! conn "test-ledger:main")]
              (is (util/exception? result))
              (is (re-find #"Cannot delete the main branch" (ex-message result)))
              (is (re-find #"Use the drop API" (ex-message result))
                  "Error should suggest using drop API"))))

        (finally
          @(fluree/disconnect conn))))))

(deftest ^:integration branch-isolation-scenario-test
  (testing "Complete branch isolation scenario"
    (let [conn        (test-utils/create-conn)
          ledger-name "scenario-test"]

      ;; Create a new ledger without branch name (defaults to :main)
      (testing "Create ledger without branch name (should default to :main)"
        @(fluree/create conn ledger-name {})
        (let [db @(fluree/load conn ledger-name)]
          (is (some? db) "Database should be loaded")
          (is (= 0 (:t db)) "Database should be at t=0 initially")))

      ;; Insert data into the ledger (should go to main branch)
      (testing "Insert data into main branch"
        (let [result @(fluree/insert! conn ledger-name
                                      {"@context" {"ex" "http://example.org/"}
                                       "@graph" [{"@id" "ex:alice"
                                                  "@type" "ex:Person"
                                                  "ex:name" "Alice"
                                                  "ex:age" 30}]})]
          (is (some? result) "Insert should return a result")
          (is (= 1 (:t result)) "Should be at t=1 after first insert")))

      ;; Verify data is in main branch
      (testing "Query main branch to verify Alice is there"
        (let [db @(fluree/load conn ledger-name)
              result @(fluree/query db {"@context" {"ex" "http://example.org/"}
                                        "select" ["?name" "?age"]
                                        "where" {"@id" "ex:alice"
                                                 "ex:name" "?name"
                                                 "ex:age" "?age"}})]
          (is (= [["Alice" 30]] result))))

      ;; Create a branch from main
      (testing "Create feature branch from main"
        (let [result @(fluree/create-branch! conn (str ledger-name ":feature") (str ledger-name ":main"))]
          (is (some? result) "Create branch should return a result")
          (is (= "feature" (:name result)) "Branch name should be 'feature'")
          (is (some? (:head result)) "Feature branch should have a head commit")))

      ;; Insert more data into the new branch
      (testing "Insert data into feature branch"
        (let [result @(fluree/insert! conn (str ledger-name ":feature")
                                      {"@context" {"ex" "http://example.org/"}
                                       "@graph" [{"@id" "ex:bob"
                                                  "@type" "ex:Person"
                                                  "ex:name" "Bob"
                                                  "ex:age" 25}]})]
          (is (some? result) "Insert to feature branch should return a result")
          (is (= 2 (:t result)) "Feature branch should be at t=2 after insert")))

      ;; Query both branches and verify different results
      (testing "Query both branches to see different results"
        ;; Query main branch - should only have Alice
        (let [main-db @(fluree/load conn (str ledger-name ":main"))
              main-result @(fluree/query main-db {"@context" {"ex" "http://example.org/"}
                                                  "select" "?name"
                                                  "where" {"@id" "?id"
                                                           "@type" "ex:Person"
                                                           "ex:name" "?name"}})]
          (is (= ["Alice"] main-result) "Main branch should only have Alice"))

        ;; Query feature branch - should have both Alice and Bob
        (let [feature-db @(fluree/load conn (str ledger-name ":feature"))
              feature-result @(fluree/query feature-db {"@context" {"ex" "http://example.org/"}
                                                        "select" "?name"
                                                        "where" {"@id" "?id"
                                                                 "@type" "ex:Person"
                                                                 "ex:name" "?name"}})]
          (is (= ["Alice" "Bob"] feature-result) "Feature branch should have both Alice and Bob"))))))

(deftest ^:integration branch-multiple-branches-test
  (testing "Create and manage multiple branches"
    (let [conn @(fluree/connect-memory {})]
      (try
        ;; Create ledger with initial data
        @(fluree/create conn "multi-branch" {})
        @(fluree/insert! conn "multi-branch"
                         {"@context" {"ex" "http://example.org/"}
                          "@graph" [{"@id" "ex:root" "ex:value" "initial"}]})

        ;; Create branches and add data strategically
        (testing "Create dev branch and add data"
          @(fluree/create-branch! conn "multi-branch:dev" "multi-branch:main")
          @(fluree/insert! conn "multi-branch:dev"
                           {"@context" {"ex" "http://example.org/"}
                            "@graph" [{"@id" "ex:dev-data" "ex:value" "dev"}]}))

        (testing "Create staging branch from main"
          @(fluree/create-branch! conn "multi-branch:staging" "multi-branch:main")
          @(fluree/insert! conn "multi-branch:staging"
                           {"@context" {"ex" "http://example.org/"}
                            "@graph" [{"@id" "ex:staging-data" "ex:value" "staging"}]}))

        (testing "Create feature-1 branch from dev (after dev has data)"
          @(fluree/create-branch! conn "multi-branch:feature-1" "multi-branch:dev")
          @(fluree/insert! conn "multi-branch:feature-1"
                           {"@context" {"ex" "http://example.org/"}
                            "@graph" [{"@id" "ex:feature-data" "ex:value" "feature"}]}))

        ;; Verify each branch has the expected data
        (testing "Verify branch isolation across multiple branches"
          ;; Main branch - only root data
          (let [main-db @(fluree/load conn "multi-branch:main")
                main-ids @(fluree/query main-db {"@context" {"ex" "http://example.org/"}
                                                 "select" "?id"
                                                 "where" {"@id" "?id"
                                                          "ex:value" "?v"}})] ;; Filter to only user data
            (is (= ["ex:root"] main-ids) "Main should only have root data"))

          ;; Dev branch - root + dev data
          (let [dev-db @(fluree/load conn "multi-branch:dev")
                dev-ids @(fluree/query dev-db {"@context" {"ex" "http://example.org/"}
                                               "select" "?id"
                                               "where" {"@id" "?id"
                                                        "ex:value" "?v"}})] ;; Filter to only user data
            (is (= #{"ex:root" "ex:dev-data"} (set dev-ids))
                "Dev should have root and dev data"))

          ;; Feature-1 branch - root + dev + feature data (branched from dev)
          (let [feature-db @(fluree/load conn "multi-branch:feature-1")
                feature-ids @(fluree/query feature-db {"@context" {"ex" "http://example.org/"}
                                                       "select" "?id"
                                                       "where" {"@id" "?id"
                                                                "ex:value" "?v"}})] ;; Filter to only user data
            (is (= #{"ex:root" "ex:dev-data" "ex:feature-data"} (set feature-ids))
                "Feature-1 should have root, dev, and feature data")))

        (finally
          @(fluree/disconnect conn))))))

(deftest ^:integration branch-filesystem-persistence-test
  (testing "Branch operations with filesystem persistence"
    (with-temp-dir [storage-path {}]
      (let [conn-opts {:storage-path (str storage-path)}]

        (testing "Create and persist branches to filesystem"
          (let [conn @(fluree/connect-file conn-opts)]
            (try
              ;; Create ledger and add data to main branch
              @(fluree/create conn "persist-test" {})
              @(fluree/insert! conn "persist-test"
                               {"@context" {"ex" "http://example.org/"}
                                "@graph" [{"@id" "ex:main-data"
                                           "@type" "MainData"
                                           "ex:value" "main-branch-value"}]})

              ;; Create feature branch
              @(fluree/create-branch! conn "persist-test:feature" "persist-test:main")

              ;; Add different data to feature branch
              @(fluree/insert! conn "persist-test:feature"
                               {"@context" {"ex" "http://example.org/"}
                                "@graph" [{"@id" "ex:feature-data"
                                           "@type" "FeatureData"
                                           "ex:value" "feature-branch-value"}]})

              ;; Store branch info for later verification
              (let [main-info @(fluree/branch-info conn "persist-test:main")
                    feature-info @(fluree/branch-info conn "persist-test:feature")]
                (is (= "main" (:name main-info)))
                (is (= "feature" (:name feature-info)))
                (is (some? (:head main-info)))
                (is (some? (:head feature-info)))

                ;; Disconnect
                @(fluree/disconnect conn)

                ;; Reconnect and verify persistence
                (testing "Verify branches persist after reconnection"
                  (let [conn2 @(fluree/connect-file conn-opts)]
                    (try
                      ;; Load and query main branch
                      (let [main-db @(fluree/load conn2 "persist-test:main")
                            main-results @(fluree/query main-db {"@context" {"ex" "http://example.org/"}
                                                                 "select" "?value"
                                                                 "where" {"@id" "ex:main-data"
                                                                          "ex:value" "?value"}})]
                        (is (= ["main-branch-value"] main-results)
                            "Main branch data should persist"))

                      ;; Load and query feature branch
                      (let [feature-db @(fluree/load conn2 "persist-test:feature")
                            feature-results @(fluree/query feature-db {"@context" {"ex" "http://example.org/"}
                                                                       "select" "?id"
                                                                       "where" {"@id" "?id"
                                                                                "@type" "?type"}})]
                        (is (= #{"ex:main-data" "ex:feature-data"} (set feature-results))
                            "Feature branch should have both main and feature data"))

                      (finally
                        @(fluree/disconnect conn2))))))

              (finally
                @(fluree/disconnect conn)))))))))
