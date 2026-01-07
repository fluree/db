(ns fluree.db.flake.index.dedupe-test
  "Tests that duplicate same-op flakes are deduplicated during indexing.

   Problem: When the same fact is asserted multiple times across different
   commits (e.g., asserting {:name \"Alice\"} three times), without deduplication
   we'd end up with multiple flakes in the index that all represent the same
   true assertion:
     [s p o dt t=1 op=true]
     [s p o dt t=2 op=true]
     [s p o dt t=3 op=true]

   The dedup fix ensures only the earliest assertion is kept, as subsequent
   same-op assertions are redundant - the fact is already true."
  (:require [clojure.core.async :as async]
            [clojure.test :refer [deftest testing is]]
            [fluree.db.api :as fluree]
            [fluree.db.flake :as flake]
            [fluree.db.flake.index :as index]
            [fluree.db.test-utils :as test-utils]
            [fluree.db.util.async :refer [<??]]))

(defn collect-leaf-flakes
  "Recursively collect all flakes from leaf nodes in an index.
   Returns a sequence of all flakes in the index."
  [index-catalog node]
  (let [resolved (if (index/resolved? node)
                   node
                   (<?? (index/resolve index-catalog node)))]
    (if (index/leaf? resolved)
      ;; Leaf node - return its flakes
      (seq (:flakes resolved))
      ;; Branch node - recurse into children
      (let [children (:children resolved)]
        (mapcat (fn [[_k child]]
                  (collect-leaf-flakes index-catalog child))
                children)))))

(defn group-flakes-by-fact
  "Group flakes by their fact identity (s, p, o, dt).
   Returns a map of [s p o dt] -> [flakes with that identity]"
  [flakes]
  (group-by (fn [f]
              [(flake/s f) (flake/p f) (flake/o f) (flake/dt f)])
            flakes))

(defn find-duplicate-assertions
  "Find flakes where the same fact (s, p, o, dt) has multiple assertions (op=true).
   Returns a map of [s p o dt] -> count of true assertions, for any with count > 1."
  [flakes]
  (let [grouped (group-flakes-by-fact flakes)]
    (->> grouped
         (map (fn [[fact-key fact-flakes]]
                (let [true-count (count (filter flake/op fact-flakes))]
                  (when (> true-count 1)
                    [fact-key true-count]))))
         (remove nil?)
         (into {}))))

(defn- base-leaf
  [cmp]
  ;; add-flakes operates on a *resolved* leaf: :flakes must be a sorted-set, not nil.
  (assoc (index/empty-leaf "dedupe-test" cmp)
         :id :test
         :flakes (flake/sorted-set-by cmp)
         :size 0
         :first flake/maximum))

(deftest ^:integration dedupe-same-assertion-test
  (testing "Duplicate assertions of the same fact are deduplicated"
    (let [;; Create connection with indexing enabled, small thresholds to trigger indexing
          conn @(fluree/connect-memory {:defaults {:indexing {:reindex-min-bytes 100
                                                              :reindex-max-bytes 10000}}})
          _    @(fluree/create conn "dedupe-test")]

      ;; Commit the SAME data 3 times in separate commits
      ;; This simulates the scenario where the same assertion is made multiple times
      (doseq [i (range 3)]
        (let [txn {"@context" {"ex" "http://example.org/"}
                   ;; Same exact data every time
                   "insert"   [{"@id"     "ex:alice"
                                "@type"   "ex:Person"
                                "ex:name" "Alice"
                                "ex:age"  30}]}
              db  @(fluree/update @(fluree/db conn "dedupe-test") txn)]
          ;; For the last commit, wait for indexing to complete
          (if (= i 2)
            (let [index-ch (async/chan 10)]
              @(fluree/commit! conn db {:index-files-ch index-ch})
              (async/<!! (test-utils/block-until-index-complete index-ch)))
            @(fluree/commit! conn db))))

      ;; Get the indexed db
      (let [db           @(fluree/db conn "dedupe-test")
            _            (is (= 3 (:t db)) "Should be at t=3 after 3 commits")
            index-catalog (:index-catalog db)
            spot-root    (:spot db)]

        (testing "Index exists and is queryable"
          (is (some? spot-root) "Should have a SPOT index root")
          (let [result @(fluree/query db
                                      {:context {"ex" "http://example.org/"}
                                       :select  ["?name"]
                                       :where   [{"@id"     "?person"
                                                  "@type"   "ex:Person"
                                                  "ex:name" "?name"}]})]
            (is (= 1 (count result)) "Should return 1 person (Alice)")
            (is (= "Alice" (first (first result))) "Should be Alice")))

        (testing "Flakes are deduplicated - no duplicate same-op assertions"
          (let [all-flakes       (collect-leaf-flakes index-catalog spot-root)
                duplicates       (find-duplicate-assertions all-flakes)]
            ;; The key assertion: there should be NO duplicate same-op assertions
            ;; Each fact should have at most 1 assertion (op=true) flake
            (is (empty? duplicates)
                (str "Should have no duplicate same-op assertions, but found: " duplicates))))))))

(deftest ^:integration dedupe-with-retractions-test
  (testing "Dedup preserves proper assert/retract sequences"
    (let [conn @(fluree/connect-memory {:defaults {:indexing {:reindex-min-bytes 100
                                                              :reindex-max-bytes 10000}}})
          _    @(fluree/create conn "dedupe-retract-test")]

      ;; Commit 1: Assert Alice with age 30
      (let [txn1 {"@context" {"ex" "http://example.org/"}
                  "insert"   [{"@id"     "ex:alice"
                               "@type"   "ex:Person"
                               "ex:age"  30}]}
            db1  @(fluree/update @(fluree/db conn "dedupe-retract-test") txn1)]
        @(fluree/commit! conn db1))

      ;; Commit 2: Change age to 31 (implicit retract of 30, assert 31)
      (let [txn2 {"@context" {"ex" "http://example.org/"}
                  "delete"   [{"@id" "ex:alice" "ex:age" 30}]
                  "insert"   [{"@id" "ex:alice" "ex:age" 31}]}
            db2  @(fluree/update @(fluree/db conn "dedupe-retract-test") txn2)]
        @(fluree/commit! conn db2))

      ;; Commit 3: Re-assert age 30 (assert 30 again, retract 31)
      (let [txn3 {"@context" {"ex" "http://example.org/"}
                  "delete"   [{"@id" "ex:alice" "ex:age" 31}]
                  "insert"   [{"@id" "ex:alice" "ex:age" 30}]}
            db3  @(fluree/update @(fluree/db conn "dedupe-retract-test") txn3)
            index-ch (async/chan 10)]
        @(fluree/commit! conn db3 {:index-files-ch index-ch})
        (async/<!! (test-utils/block-until-index-complete index-ch)))

      ;; Query should return current state correctly
      (let [db     @(fluree/db conn "dedupe-retract-test")
            result @(fluree/query db
                                  {:context {"ex" "http://example.org/"}
                                   :select  ["?age"]
                                   :where   [{"@id"    "ex:alice"
                                              "ex:age" "?age"}]})]
        (is (= 1 (count result)) "Should return 1 age value")
        (is (= 30 (first (first result))) "Current age should be 30")))))

(deftest add-flakes-dedup-keeps-earliest-same-op
  (testing "add-flakes drops redundant same-op reassertions and keeps earliest assertion"
    (let [cmp  flake/cmp-flakes-spot
          leaf (base-leaf cmp)
          f1   (flake/create 100 200 "value" 1 1 true nil)   ; assert at t=1
          f2   (flake/create 100 200 "value" 1 2 true nil)   ; redundant assert at t=2
          f3   (flake/create 100 200 "value" 1 3 true nil)   ; redundant assert at t=3
          leaf* (index/add-flakes leaf [f1 f2 f3])
          flakes* (:flakes leaf*)]
      (is (= 1 (count flakes*)) "Only one flake should remain after dedup")
      (is (= 1 (flake/t (first flakes*))) "Remaining flake should be the earliest assertion (t=1)"))))

(deftest add-flakes-all-incoming-redundant-preserves-non-leftmost-first
  (testing "If all incoming flakes are redundant, a non-leftmost leaf preserves :first and :size"
    (let [cmp    flake/cmp-flakes-spot
          f-old  (flake/create 1 2 3 4 1 true nil)
          f-new1 (flake/create 1 2 3 4 2 true nil)
          f-new2 (flake/create 1 2 3 4 3 true nil)
          leaf   (-> (base-leaf cmp)
                     (assoc :leftmost? false
                            :flakes (flake/sorted-set-by cmp f-old)
                            :first f-old
                            :size (flake/size-flake f-old)))
          leaf*  (index/add-flakes leaf [f-new1 f-new2])]
      (is (= (:first leaf) (:first leaf*)) "Non-leftmost leaf :first should not change")
      (is (= (:size leaf) (:size leaf*)) "Leaf size should be unchanged when all new flakes are redundant")
      (is (= 1 (count (:flakes leaf*))) "Leaf should still contain a valid flake set")
      (is (= f-old (first (:flakes leaf*))) "Original flake should be retained"))))

(deftest add-flakes-mixed-ops-preserves-sequence
  (testing "add-flakes preserves correct assert/retract sequences"
    (let [cmp  flake/cmp-flakes-spot
          leaf (base-leaf cmp)

          ;; Create a proper sequence: assert, retract, assert
          f1   (flake/create 100 200 "value" 1 1 true nil)   ; assert at t=1
          f2   (flake/create 100 200 "value" 1 2 false nil)  ; retract at t=2
          f3   (flake/create 100 200 "value" 1 3 true nil)   ; re-assert at t=3

          result-leaf (index/add-flakes leaf [f1 f2 f3])
          result-flakes (:flakes result-leaf)]

      (testing "All three flakes should be preserved (different ops)"
        (is (= 3 (count result-flakes))
            "Assert-retract-assert sequence should preserve all flakes"))

      (testing "Flakes are in correct order"
        (let [flake-vec (vec result-flakes)]
          (is (= true (flake/op (nth flake-vec 0))) "First should be assert")
          (is (= false (flake/op (nth flake-vec 1))) "Second should be retract")
          (is (= true (flake/op (nth flake-vec 2))) "Third should be assert"))))))
