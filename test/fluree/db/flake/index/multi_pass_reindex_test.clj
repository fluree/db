(ns fluree.db.flake.index.multi-pass-reindex-test
  "Tests that multi-pass indexing (reindex) correctly preserves children without novelty.

   The fix under test: `merge-with-unchanged-children` in novelty.cljc

   Problem: During multi-pass indexing, children without novelty were being lost
   because tree-chan filters children by the `novel?` predicate. Children without
   novelty were never pushed to the integration stack and therefore never included
   when the parent branch was reconstructed.

   This test verifies:
   1. Multi-pass reindex completes without errors
   2. The sibling boundary invariant holds: child[i].rhs == child[i+1].first
   3. All data is preserved (no data loss)
   4. Query results are correct after reindex"
  (:require [babashka.fs :refer [with-temp-dir]]
            [clojure.test :refer [deftest testing is]]
            [fluree.db.api :as fluree]
            [fluree.db.flake.index :as index]
            [fluree.db.util.async :refer [<??]]))

(defn check-sibling-boundaries
  "Check that child[i].rhs == child[i+1].first for all adjacent children.
   Returns a vector of error maps for any violations found."
  [branch]
  (let [children   (:children branch)
        comparator (:comparator branch)]
    (when (and comparator (map? children) (> (count children) 1))
      (let [child-vals (vec (vals (sort-by key comparator children)))]
        (->> (range (dec (count child-vals)))
             (keep (fn [i]
                     (let [child-a   (nth child-vals i)
                           child-b   (nth child-vals (inc i))
                           rhs-a     (:rhs child-a)
                           first-b   (:first child-b)]
                       (when (and rhs-a first-b
                                  (not (zero? (comparator rhs-a first-b))))
                         {:error       :sibling-boundary-mismatch
                          :branch-id   (:id branch)
                          :child-i     i
                          :child-a-id  (:id child-a)
                          :child-b-id  (:id child-b)}))))
             vec)))))

(defn check-branch-recursive
  "Recursively check a resolved branch for sibling boundary invariants.
   Returns {:errors [...] :stats {...}}"
  [index-catalog branch depth]
  (let [boundary-errors (check-sibling-boundaries branch)
        children        (:children branch)]
    (if (or (nil? children)
            (empty? children)
            (some #(true? (:leaf (val %))) children))
      ;; Children are leaves or no children - we're done
      {:errors boundary-errors
       :stats  {:depth depth
                :branches-checked 1}}
      ;; Children are branches - resolve and recurse
      (let [child-results
            (reduce
             (fn [results [_k child]]
               (let [resolved-child (if (index/resolved? child)
                                      child
                                      (<?? (index/resolve index-catalog child)))
                     result         (check-branch-recursive index-catalog resolved-child (inc depth))]
                 (conj results result)))
             []
             children)
            all-errors     (into boundary-errors (mapcat :errors child-results))
            total-branches (+ 1 (reduce + (map #(get-in % [:stats :branches-checked] 0) child-results)))
            max-depth      (reduce max depth (map #(get-in % [:stats :depth] 0) child-results))]
        {:errors all-errors
         :stats  {:branches-checked total-branches
                  :depth            max-depth}}))))

(defn check-index-consistency
  "Check consistency of an index in the given db.
   Returns {:errors [...] :stats {...}}"
  [db idx-type]
  (let [index-catalog (:index-catalog db)
        root          (get db idx-type)]
    (if-not root
      {:errors [{:error :no-root :idx idx-type}]
       :stats  {:branches-checked 0}}
      (let [resolved-root (if (index/resolved? root)
                            root
                            (<?? (index/resolve index-catalog root)))]
        (check-branch-recursive index-catalog resolved-root 0)))))

(defn check-all-indexes
  "Check all indexes for sibling boundary invariants.
   Returns map of idx-type -> {:errors [...] :stats {...}}"
  [db]
  (reduce (fn [results idx]
            (assoc results idx (check-index-consistency db idx)))
          {}
          [:spot :psot :post :opst :tspo]))

(deftest ^:integration multi-pass-reindex-preserves-unchanged-children-test
  (testing "Multi-pass reindex correctly merges unchanged children"
    (with-temp-dir [storage-path {}]
      (let [;; Create connection with indexing disabled initially
            conn @(fluree/connect-file {:storage-path (str storage-path)
                                        :defaults {:indexing {:indexing-enabled false
                                                              :reindex-min-bytes 1000
                                                              :reindex-max-bytes 10000000}}})
            _    @(fluree/create conn "multi-pass-test")]

        ;; Insert enough data to create multiple index nodes
        ;; We need enough data that when reindexed with small batches,
        ;; some children will have novelty and some won't
        (doseq [batch (range 10)]
          (let [txn {"@context" {"ex" "http://example.org/"}
                     "insert"   (vec
                                 (for [i (range 20)]
                                   {"@id"       (str "ex:entity-" batch "-" i)
                                    "@type"     "ex:Entity"
                                    "ex:name"   (str "Entity " batch "-" i)
                                    "ex:batch"  batch
                                    "ex:index"  i
                                    "ex:desc"   (str "This is a longer description for entity "
                                                     batch "-" i " to increase the data size")}))}
                db  @(fluree/update @(fluree/db conn "multi-pass-test") txn)]
            @(fluree/commit! conn db)))

        (let [pre-db @(fluree/db conn "multi-pass-test")]
          (testing "Pre-reindex state"
            (is (= 10 (:t pre-db)) "Should be at t=10 after 10 transactions")))

        (testing "Reindex with small batch-bytes forces multiple passes"
          ;; Use small batch-bytes to force multiple indexing passes
          ;; The key is that some nodes will have novelty in pass N but not in pass N+1
          (let [reindexed-db @(fluree/reindex conn "multi-pass-test"
                                              {:batch-bytes 2000})] ;; Force many batches

            (testing "Final state is consistent"
              (let [final-results (check-all-indexes reindexed-db)
                    total-errors  (reduce + (map #(count (:errors %)) (vals final-results)))]
                (is (zero? total-errors)
                    (str "Final state should have no errors: " final-results))))

            (testing "All data is preserved"
              (let [result @(fluree/query reindexed-db
                                          {:context {"ex" "http://example.org/"}
                                           :select  ["?id"]
                                           :where   [{"@id"   "?id"
                                                      "@type" "ex:Entity"}]})]
                (is (= 200 (count result)) "Should have all 200 entities (10 batches × 20 entities)")))))

        (testing "Reload from disk and query works"
          (let [conn2      @(fluree/connect-file {:storage-path (str storage-path)
                                                  :defaults {:indexing {:indexing-enabled false}}})
                _          @(fluree/load conn2 "multi-pass-test")
                loaded-db  @(fluree/db conn2 "multi-pass-test")]

            (testing "Loaded db has correct t"
              (is (= 10 (:t loaded-db)) "Should be at t=10"))

            (testing "Query works after reload"
              (let [result @(fluree/query loaded-db
                                          {:context {"ex" "http://example.org/"}
                                           :select  ["?name"]
                                           :where   [{"@id"     "?entity"
                                                      "@type"   "ex:Entity"
                                                      "ex:name" "?name"}]})]
                (is (= 200 (count result)) "Should return all 200 entity names")))))))))

(deftest ^:integration multi-pass-reindex-memory-test
  (testing "Multi-pass reindex works with memory storage"
    (let [conn @(fluree/connect-memory {:defaults {:indexing {:indexing-enabled false
                                                              :reindex-min-bytes 500
                                                              :reindex-max-bytes 10000000}}})
          _    @(fluree/create conn "memory-test")]

      ;; Insert data in multiple transactions
      (doseq [batch (range 5)]
        (let [txn {"@context" {"ex" "http://example.org/"}
                   "insert"   (vec
                               (for [i (range 10)]
                                 {"@id"     (str "ex:item-" batch "-" i)
                                  "@type"   "ex:Item"
                                  "ex:name" (str "Item " batch "-" i)
                                  "ex:value" (+ (* batch 100) i)}))}
              db  @(fluree/update @(fluree/db conn "memory-test") txn)]
          @(fluree/commit! conn db)))

      (testing "Reindex completes successfully"
        (let [reindexed-db @(fluree/reindex conn "memory-test"
                                            {:batch-bytes 1000})]

          (is (= 5 (:t reindexed-db)) "Should be at t=5")

          (testing "All data is queryable"
            (let [result @(fluree/query reindexed-db
                                        {:context {"ex" "http://example.org/"}
                                         :select  ["?id"]
                                         :where   [{"@id"   "?id"
                                                    "@type" "ex:Item"}]})]
              (is (= 50 (count result)) "Should have all 50 items"))))))))
