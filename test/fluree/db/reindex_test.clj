(ns fluree.db.reindex-test
  "Tests for reindex API functionality - rebuilding indexes from commit history."
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest testing is]]
            [fluree.db.api :as fluree]
            [fluree.db.test-utils :as test-utils]))

(deftest ^:integration reindex-basic-test
  (testing "Reindex API rebuilds index with statistics"
    (let [;; Create connection with auto-indexing disabled to simulate v1 scenario
          conn @(fluree/connect-memory {:defaults {:indexing {:indexing-enabled false
                                                              :reindex-min-bytes 100
                                                              :reindex-max-bytes 10000}}})
          _    @(fluree/create conn "reindex-test")

          ;; Insert some test data
          txn1 {"@context" {"ex" "http://example.org/"}
                "insert"   [{"@id"      "ex:alice"
                             "@type"    "ex:Person"
                             "ex:name"  "Alice"
                             "ex:age"   30}
                            {"@id"      "ex:bob"
                             "@type"    "ex:Person"
                             "ex:name"  "Bob"
                             "ex:age"   25}]}
          txn2 {"@context" {"ex" "http://example.org/"}
                "insert"   [{"@id"      "ex:charlie"
                             "@type"    "ex:Person"
                             "ex:name"  "Charlie"
                             "ex:age"   35}]}

          ;; Commit both transactions
          db0     @(fluree/db conn "reindex-test")
          db1     @(fluree/update db0 txn1)
          _       @(fluree/commit! conn db1)
          db2     @(fluree/update @(fluree/db conn "reindex-test") txn2)
          _       @(fluree/commit! conn db2)

          ;; Check initial state - should have no stats since indexing disabled
          pre-db  @(fluree/db conn "reindex-test")]

      (testing "Pre-reindex state has minimal/no statistics"
        (is (= 2 (:t pre-db)) "Should be at t=2")
        ;; Either no :stats key or empty property stats
        (is (or (nil? (:stats pre-db))
                (empty? (get-in pre-db [:stats :properties])))
            "Should have no property statistics before reindex"))

      (testing "Reindex rebuilds the index from commits"
        (let [reindexed-db @(fluree/reindex conn "reindex-test")]
          (is (= 2 (:t reindexed-db)) "Should still be at t=2 after reindex")

          (testing "Statistics are populated after reindex"
            (let [stats (:stats reindexed-db)]
              (is (some? stats) "Should have :stats after reindex")
              (is (map? (:properties stats)) "Should have property stats map")
              (is (pos? (count (:properties stats))) "Should have property statistics")
              (is (map? (:classes stats)) "Should have class stats map")
              (is (pos? (count (:classes stats))) "Should have class statistics")))))

      (testing "Query still works after reindex"
        (let [result @(fluree/query @(fluree/db conn "reindex-test")
                                    {:context {"ex" "http://example.org/"}
                                     :select  ["?name"]
                                     :where   [{"@id"   "?person"
                                                "@type" "ex:Person"
                                                "ex:name" "?name"}]})]
          (is (= 3 (count result)) "Should return 3 people"))))))

(deftest ^:integration reindex-batch-processing-test
  (testing "Reindex processes commits in batches based on batch-bytes"
    (let [;; Use larger batch size to avoid novelty limit issues during normal commits
          conn @(fluree/connect-memory {:defaults {:indexing {:indexing-enabled false
                                                              :reindex-min-bytes 10000
                                                              :reindex-max-bytes 1000000}}})
          _    @(fluree/create conn "reindex-batch-test")]

      ;; Insert multiple transactions to generate enough commits
      (doseq [i (range 5)]
        (let [txn {"@context" {"ex" "http://example.org/"}
                   "insert"   [{"@id"      (str "ex:person" i)
                                "@type"    "ex:Person"
                                "ex:name"  (str "Person " i)
                                "ex:age"   (+ 20 i)
                                "ex:email" (str "person" i "@example.com")}]}
              db  @(fluree/update @(fluree/db conn "reindex-batch-test") txn)]
          @(fluree/commit! conn db)))

      (let [pre-db @(fluree/db conn "reindex-batch-test")]
        (is (= 5 (:t pre-db)) "Should be at t=5 after 5 transactions"))

      (testing "Reindex with small batch-bytes completes successfully"
        ;; Use small batch-bytes during reindex to force multiple batches
        (let [reindexed-db @(fluree/reindex conn "reindex-batch-test"
                                            {:batch-bytes 1000})]
          (is (= 5 (:t reindexed-db)) "Should be at t=5 after reindex")
          (is (pos? (count (get-in reindexed-db [:stats :properties])))
              "Should have property statistics"))))))

(deftest ^:integration reindex-from-t-test
  (testing "Reindex with default from-t starts from t=1"
    (let [conn @(fluree/connect-memory {:defaults {:indexing {:indexing-enabled false
                                                              :reindex-min-bytes 100
                                                              :reindex-max-bytes 10000}}})
          _    @(fluree/create conn "reindex-from-t-test")]

      ;; Insert 3 transactions
      (doseq [i (range 3)]
        (let [txn {"@context" {"ex" "http://example.org/"}
                   "insert"   [{"@id"      (str "ex:item" i)
                                "@type"    "ex:Item"
                                "ex:label" (str "Item " i)}]}
              db  @(fluree/update @(fluree/db conn "reindex-from-t-test") txn)]
          @(fluree/commit! conn db)))

      (testing "Default reindex from t=1 includes all user data"
        (let [reindexed-db @(fluree/reindex conn "reindex-from-t-test")]
          (is (= 3 (:t reindexed-db)) "Should be at t=3")
          (let [result @(fluree/query reindexed-db
                                      {:context {"ex" "http://example.org/"}
                                       :select  ["?label"]
                                       :where   [{"@id"      "?item"
                                                  "@type"    "ex:Item"
                                                  "ex:label" "?label"}]})]
            (is (= 3 (count result)) "Should have all 3 items")))))))

(deftest ^:integration reindex-preserves-queries-test
  (testing "Reindex preserves basic query functionality"
    (let [conn @(fluree/connect-memory {:defaults {:indexing {:indexing-enabled false
                                                              :reindex-min-bytes 100
                                                              :reindex-max-bytes 10000}}})
          _    @(fluree/create conn "reindex-query-test")

          ;; Insert data
          txn {"@context" {"ex" "http://example.org/"}
               "insert"   [{"@id"       "ex:emp1"
                            "@type"     "ex:Employee"
                            "ex:name"   "Alice"
                            "ex:salary" 75000}
                           {"@id"       "ex:emp2"
                            "@type"     "ex:Employee"
                            "ex:name"   "Bob"
                            "ex:salary" 65000}]}
          db  @(fluree/update @(fluree/db conn "reindex-query-test") txn)
          _   @(fluree/commit! conn db)

          ;; Reindex
          reindexed-db @(fluree/reindex conn "reindex-query-test")]

      (testing "Basic select query works"
        (let [result @(fluree/query reindexed-db
                                    {:context {"ex" "http://example.org/"}
                                     :select  ["?name"]
                                     :where   [{"@id"     "?emp"
                                                "@type"   "ex:Employee"
                                                "ex:name" "?name"}]})]
          (is (= 2 (count result)) "Should return 2 employees")))

      (testing "Query with filter works"
        (let [result @(fluree/query reindexed-db
                                    {:context {"ex" "http://example.org/"}
                                     :select  ["?name"]
                                     :where   [{"@id"       "?emp"
                                                "@type"     "ex:Employee"
                                                "ex:name"   "?name"
                                                "ex:salary" "?salary"}
                                               [:filter "(> ?salary 70000)"]]})]
          (is (= 1 (count result)) "Should return 1 employee with salary > 70000"))))))

(deftest ^:integration reindex-with-existing-index-test
  (testing "Reindex replaces existing v1 index with v2 index containing statistics"
    (let [;; Create connection with indexing ENABLED (small thresholds to trigger quickly)
          conn        @(fluree/connect-memory {:defaults {:indexing {:reindex-min-bytes 100
                                                                     :reindex-max-bytes 10000}}})
          _           @(fluree/create conn "reindex-existing")

          ;; Insert data
          txn         {"@context" {"ex" "http://example.org/"}
                       "insert"   [{"@id"     "ex:alice"
                                    "@type"   "ex:Person"
                                    "ex:name" "Alice"
                                    "ex:age"  30}
                                   {"@id"     "ex:bob"
                                    "@type"   "ex:Person"
                                    "ex:name" "Bob"
                                    "ex:age"  25}]}
          db0         @(fluree/db conn "reindex-existing")
          db1         @(fluree/update db0 txn)

          ;; Commit with index-files-ch to wait for indexing
          index-ch    (async/chan 10)
          _           @(fluree/commit! conn db1 {:index-files-ch index-ch})
          _           (async/<!! (test-utils/block-until-index-complete index-ch))

          ;; Get the db with the initial index
          pre-db      @(fluree/db conn "reindex-existing")
          old-index-addr (get-in pre-db [:commit :index :address])
          old-index-t    (get-in pre-db [:commit :index :data :t])]

      (testing "Pre-reindex: index exists"
        (is (some? old-index-addr) "Should have an index address before reindex")
        (is (= 1 old-index-t) "Index should be at t=1"))

      (testing "Pre-reindex: v2 index already has stats (from enabled indexing)"
        ;; When indexing is enabled, v2 is the default and stats are computed
        (is (pos? (count (get-in pre-db [:stats :properties])))
            "v2 index should already have property stats"))

      (testing "Reindex creates new index, replacing the old one"
        (let [reindexed-db     @(fluree/reindex conn "reindex-existing")
              new-index-addr   (get-in reindexed-db [:commit :index :address])
              new-index-t      (get-in reindexed-db [:commit :index :data :t])]

          (is (some? new-index-addr) "Should have an index address after reindex")
          (is (= 1 new-index-t) "Index should still be at t=1 (same data)")

          ;; The new index should be at a DIFFERENT storage address
          ;; (even though same 't' value, since it was rebuilt)
          (is (not= old-index-addr new-index-addr)
              "New index should be at different address than old index")

          (testing "Statistics are present after reindex"
            (is (pos? (count (get-in reindexed-db [:stats :properties])))
                "Should have property statistics")
            (is (pos? (count (get-in reindexed-db [:stats :classes])))
                "Should have class statistics"))

          (testing "Query works with new index"
            (let [result @(fluree/query @(fluree/db conn "reindex-existing")
                                        {:context {"ex" "http://example.org/"}
                                         :select  ["?name"]
                                         :where   [{"@id"   "?person"
                                                    "@type" "ex:Person"
                                                    "ex:name" "?name"}]})]
              (is (= 2 (count result)) "Should return 2 people"))))))))
