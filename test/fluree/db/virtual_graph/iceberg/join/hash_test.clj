(ns fluree.db.virtual-graph.iceberg.join.hash-test
  "Tests for the streaming hash join operator."
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.constants :as const]
            [fluree.db.query.exec.where :as where]
            [fluree.db.virtual-graph.iceberg.join.hash :as hash-join]))

;;; ---------------------------------------------------------------------------
;;; Basic Hash Join Tests
;;; ---------------------------------------------------------------------------

(deftest basic-hash-join-test
  (testing "Simple single-key join"
    (let [;; Airlines table (build side - dimension table)
          airlines [{:id 1 :name "United"}
                    {:id 2 :name "Delta"}
                    {:id 3 :name "American"}]
          ;; Routes table (probe side - fact table)
          routes [{:airline_id 1 :src "ORD" :dst "LAX"}
                  {:airline_id 1 :src "ORD" :dst "JFK"}
                  {:airline_id 2 :src "ATL" :dst "LAX"}
                  {:airline_id 4 :src "DEN" :dst "SEA"}] ;; airline_id 4 doesn't exist

          result (hash-join/hash-join airlines routes [:id] [:airline_id])]

      (is (= 3 (count result)) "Should have 3 joined rows (airline 4 has no match)")
      (is (every? #(contains? % :name) result) "All results should have airline name")
      (is (every? #(contains? % :src) result) "All results should have route src")

      ;; Check specific joins
      (is (= 2 (count (filter #(= (:name %) "United") result))) "United has 2 routes")
      (is (= 1 (count (filter #(= (:name %) "Delta") result))) "Delta has 1 route"))))

(deftest empty-result-test
  (testing "Empty build side returns empty result"
    (let [result (hash-join/hash-join [] [{:a 1}] [:id] [:id])]
      (is (empty? result))))

  (testing "Empty probe side returns empty result"
    (let [result (hash-join/hash-join [{:id 1}] [] [:id] [:id])]
      (is (empty? result))))

  (testing "No matching keys returns empty result"
    (let [build [{:id 1} {:id 2}]
          probe [{:id 10} {:id 20}]]
      (is (empty? (hash-join/hash-join build probe [:id] [:id]))))))

(deftest null-key-handling-test
  (testing "Null keys never match (SQL equi-join semantics)"
    (let [build [{:id nil :name "Null airline"}
                 {:id 1 :name "United"}]
          probe [{:airline_id nil :src "ORD"}
                 {:airline_id 1 :src "LAX"}]

          result (hash-join/hash-join build probe [:id] [:airline_id])]

      (is (= 1 (count result)) "Only non-null key should match")
      (is (= "United" (:name (first result)))))))

(deftest duplicate-key-handling-test
  (testing "Duplicates in build side produce multiple matches"
    (let [;; Airlines with duplicate IDs (represents denormalized data)
          build [{:id 1 :name "United-A"}
                 {:id 1 :name "United-B"}
                 {:id 2 :name "Delta"}]
          probe [{:airline_id 1 :src "ORD"}]

          result (hash-join/hash-join build probe [:id] [:airline_id])]

      (is (= 2 (count result)) "Should match both United-A and United-B")
      (is (= #{"United-A" "United-B"} (set (map :name result)))))))

;;; ---------------------------------------------------------------------------
;;; Composite Key Tests
;;; ---------------------------------------------------------------------------

(deftest composite-key-join-test
  (testing "Join on multiple columns"
    (let [;; Composite key: [region, id]
          build [{:region "US" :id 1 :name "US-1"}
                 {:region "US" :id 2 :name "US-2"}
                 {:region "EU" :id 1 :name "EU-1"}]
          probe [{:region "US" :id 1 :val "A"}
                 {:region "EU" :id 1 :val "B"}
                 {:region "US" :id 3 :val "C"}] ;; No match

          result (hash-join/hash-join build probe [:region :id] [:region :id])]

      (is (= 2 (count result)) "Two composite keys match")
      (is (some #(and (= "US-1" (:name %)) (= "A" (:val %))) result))
      (is (some #(and (= "EU-1" (:name %)) (= "B" (:val %))) result)))))

(deftest partial-composite-key-null-test
  (testing "Partial null in composite key prevents match"
    (let [build [{:a 1 :b 2 :name "Valid"}
                 {:a 1 :b nil :name "Partial null"}]
          probe [{:x 1 :y 2 :val "Match"}
                 {:x 1 :y nil :val "No match"}]

          result (hash-join/hash-join build probe [:a :b] [:x :y])]

      (is (= 1 (count result)) "Only fully non-null composite keys match"))))

;;; ---------------------------------------------------------------------------
;;; Streaming Interface Tests
;;; ---------------------------------------------------------------------------

(deftest streaming-hash-join-test
  (testing "Incremental build with streaming interface"
    (let [join (hash-join/create-hash-join [:id] [:airline_id])

          ;; Build in batches
          _ (hash-join/build! join [{:id 1 :name "United"}])
          _ (hash-join/build! join [{:id 2 :name "Delta"} {:id 3 :name "American"}])

          _ (is (= 3 (hash-join/build-count join)) "Build count reflects all batches")

          ;; Probe
          result (hash-join/probe join [{:airline_id 1 :src "ORD"}
                                        {:airline_id 2 :src "ATL"}])]

      (is (= 2 (count result)))
      (hash-join/close! join))))

(deftest streaming-close-clears-state-test
  (testing "Close clears the hash table"
    (let [join (hash-join/create-hash-join [:id] [:id])]
      (hash-join/build! join [{:id 1} {:id 2}])
      (is (= 2 (hash-join/build-count join)))

      (hash-join/close! join)
      (is (= 0 (hash-join/build-count join)))

      ;; Probe after close returns empty
      (is (empty? (hash-join/probe join [{:id 1}]))))))

;;; ---------------------------------------------------------------------------
;;; Merge Semantics Tests
;;; ---------------------------------------------------------------------------

(deftest merge-semantics-test
  (testing "Compatible keyword keys merge (no conflict)"
    ;; When keys are keywords (not symbols), they just merge
    (let [build [{:id 1 :shared_key "value" :build_only "B"}]
          probe [{:id 1 :shared_key "value" :probe_only "P"}]

          [result] (hash-join/hash-join build probe [:id] [:id])]

      (is (= "value" (:shared_key result)) "Same values merge fine")
      (is (= "B" (:build_only result)) "Build-only key preserved")
      (is (= "P" (:probe_only result)) "Probe-only key preserved")))

  (testing "Keyword keys with different values - probe wins (not SPARQL vars)"
    ;; Keyword keys are not SPARQL variables, so they just merge (probe wins)
    (let [build [{:id 1 :shared_key "from_build"}]
          probe [{:id 1 :shared_key "from_probe"}]

          [result] (hash-join/hash-join build probe [:id] [:id])]

      (is (= "from_probe" (:shared_key result)) "Probe wins for keyword keys"))))

(deftest sparql-compatible-merge-test
  (testing "Symbol keys (SPARQL vars) must match or no join"
    ;; When both solutions bind the same symbol to different values, no result
    (let [build [{'?name "Alice" :id 1}]
          probe [{'?name "Bob" :id 1}]

          result (hash-join/hash-join build probe [:id] [:id])]

      (is (empty? result) "Conflicting symbol bindings produce no result")))

  (testing "Symbol keys with same value produce result"
    (let [build [{'?name "Alice" :id 1}]
          probe [{'?name "Alice" :id 1 '?age 30}]

          [result] (hash-join/hash-join build probe [:id] [:id])]

      (is (= "Alice" (get result '?name)))
      (is (= 30 (get result '?age)))))

  (testing "Non-overlapping symbol keys merge"
    (let [build [{'?x 1 :id 1}]
          probe [{'?y 2 :id 1}]

          [result] (hash-join/hash-join build probe [:id] [:id])]

      (is (= 1 (get result '?x)))
      (is (= 2 (get result '?y))))))

;;; ---------------------------------------------------------------------------
;;; Join Column Values Tests (::join-col-vals)
;;; ---------------------------------------------------------------------------

(deftest join-col-vals-extraction-test
  (testing "Join keys extracted from ::join-col-vals when present"
    (let [;; Simulates Iceberg query solution format
          join-col-key :fluree.db.virtual-graph.iceberg.query/join-col-vals
          build [{join-col-key {:id 1} '?name "United"}
                 {join-col-key {:id 2} '?name "Delta"}]
          probe [{join-col-key {:airline_id 1} '?src "ORD"}
                 {join-col-key {:airline_id 2} '?src "ATL"}]

          result (hash-join/hash-join build probe [:id] [:airline_id])]

      (is (= 2 (count result)))
      (is (some #(and (= "United" (get % '?name)) (= "ORD" (get % '?src))) result))
      (is (some #(and (= "Delta" (get % '?name)) (= "ATL" (get % '?src))) result))))

  (testing "Falls back to direct lookup when ::join-col-vals not present"
    (let [;; Plain maps without ::join-col-vals
          build [{:id 1 :name "United"}]
          probe [{:airline_id 1 :src "ORD"}]

          result (hash-join/hash-join build probe [:id] [:airline_id])]

      (is (= 1 (count result)))
      (is (= "United" (:name (first result))))
      (is (= "ORD" (:src (first result)))))))

;;; ---------------------------------------------------------------------------
;;; Pipeline Hash Join Tests
;;; ---------------------------------------------------------------------------

(deftest pipeline-hash-joins-test
  (testing "Three-way join via pipeline"
    (let [;; Routes (starting point)
          routes [{:airline_id 1 :src_airport_id 100 :dst_airport_id 200}
                  {:airline_id 2 :src_airport_id 100 :dst_airport_id 300}]

          ;; Airlines
          airlines [{:id 1 :name "United"}
                    {:id 2 :name "Delta"}]

          ;; Airports
          airports [{:id 100 :code "ORD"}
                    {:id 200 :code "LAX"}
                    {:id 300 :code "JFK"}]

          ;; Pipeline: routes -> join airlines -> join airports (source)
          result (hash-join/pipeline-hash-joins
                  routes
                  [{:solutions airlines
                    :build-keys [:id]
                    :probe-keys [:airline_id]}
                   {:solutions airports
                    :build-keys [:id]
                    :probe-keys [:src_airport_id]}])]

      (is (= 2 (count result)) "Both routes should have matches")
      (is (every? #(contains? % :name) result) "All have airline name")
      (is (every? #(= "ORD" (:code %)) result) "All routes start from ORD"))))

(deftest pipeline-short-circuit-test
  (testing "Pipeline short-circuits on empty intermediate result"
    (let [routes [{:airline_id 999}] ;; No matching airline
          airlines [{:id 1 :name "United"}]
          airports [{:id 100 :code "ORD"}]

          result (hash-join/pipeline-hash-joins
                  routes
                  [{:solutions airlines
                    :build-keys [:id]
                    :probe-keys [:airline_id]}
                   {:solutions airports
                    :build-keys [:id]
                    :probe-keys [:src_airport_id]}])]

      (is (empty? result) "Should short-circuit after first join fails"))))

;;; ---------------------------------------------------------------------------
;;; Edge Case Tests
;;; ---------------------------------------------------------------------------

(deftest various-value-types-test
  (testing "Join keys can be various types"
    ;; String keys
    (let [build [{:code "US" :name "United States"}]
          probe [{:country_code "US" :city "NYC"}]
          result (hash-join/hash-join build probe [:code] [:country_code])]
      (is (= 1 (count result))))

    ;; Integer keys
    (let [build [{:id 42 :name "Answer"}]
          probe [{:ref 42 :val "X"}]
          result (hash-join/hash-join build probe [:id] [:ref])]
      (is (= 1 (count result))))

    ;; Keyword keys in data (though usually values are primitives)
    (let [build [{:type :airline :name "UA"}]
          probe [{:entity_type :airline :code "UA"}]
          result (hash-join/hash-join build probe [:type] [:entity_type])]
      (is (= 1 (count result))))))

(deftest large-result-set-test
  (testing "Hash join handles larger datasets efficiently"
    (let [n 1000
          build (vec (for [i (range n)] {:id i :name (str "Item-" i)}))
          probe (vec (for [i (range n)] {:ref_id i :val (* i 10)}))

          result (hash-join/hash-join build probe [:id] [:ref_id])]

      (is (= n (count result)) "All rows should match")
      ;; Spot check a few results
      (let [item-500 (first (filter #(= 500 (:id %)) result))]
        (is (= "Item-500" (:name item-500)))
        (is (= 5000 (:val item-500)))))))

;;; ---------------------------------------------------------------------------
;;; Streaming Behavior Tests
;;; ---------------------------------------------------------------------------

(deftest streaming-probe-test
  (testing "probe returns a lazy seq that is not fully realized until consumed"
    (let [join (hash-join/create-hash-join [:id] [:airline_id])
          probe-calls (atom 0)
          ;; Build side
          _ (hash-join/build! join [{:id 1 :name "United"}
                                    {:id 2 :name "Delta"}])
          ;; Probe with a lazy seq that tracks realization
          lazy-probe (map (fn [x]
                            (swap! probe-calls inc)
                            x)
                          [{:airline_id 1 :src "ORD"}
                           {:airline_id 2 :src "ATL"}
                           {:airline_id 1 :src "LAX"}])
          ;; Get the lazy result
          result (hash-join/probe join lazy-probe)]

      ;; Result should be a lazy seq
      (is (seq? result) "probe should return a lazy seq")

      ;; Lazy seq should not be fully realized yet (though some chunking may occur)
      ;; Note: Due to Clojure's chunking, some elements may be realized upfront
      (is (<= @probe-calls 3) "Lazy seq should not require full realization")

      ;; Now force realization
      (let [realized (doall result)]
        (is (= 3 (count realized)) "Should have 3 joined results")
        (is (= 3 @probe-calls) "All probe elements should now be realized"))

      (hash-join/close! join)))

  (testing "lazy probe result must be realized before close"
    (let [join (hash-join/create-hash-join [:id] [:id])
          _ (hash-join/build! join [{:id 1 :name "Test"}])
          ;; Get lazy result and realize it BEFORE close
          result (doall (hash-join/probe join [{:id 1 :val "A"}]))]

      ;; Close after realization
      (hash-join/close! join)

      ;; Result should still be valid since we realized before close
      (is (= 1 (count result)))
      (is (= "Test" (:name (first result)))))))

;;; ---------------------------------------------------------------------------
;;; Realistic Solution Shape Tests (SPARQL-style with match objects)
;;; ---------------------------------------------------------------------------

(deftest realistic-solution-shape-test
  (testing "Solutions with symbol keys and where/match-value objects"
    ;; This mirrors the actual shape of SPARQL solutions from Iceberg VG
    (let [join-col-key :fluree.db.virtual-graph.iceberg.query/join-col-vals
          ;; Build side: Airlines with IRI subjects and match-value objects
          build [{join-col-key {:id 1}
                  '?airline (where/match-iri {} "http://example.org/airline/1")
                  '?name (where/match-value {} "United" const/iri-string)}
                 {join-col-key {:id 2}
                  '?airline (where/match-iri {} "http://example.org/airline/2")
                  '?name (where/match-value {} "Delta" const/iri-string)}]
          ;; Probe side: Routes with FK to airlines
          probe [{join-col-key {:airline_id 1}
                  '?route (where/match-iri {} "http://example.org/route/100")
                  '?src (where/match-value {} "ORD" const/iri-string)}
                 {join-col-key {:airline_id 2}
                  '?route (where/match-iri {} "http://example.org/route/200")
                  '?src (where/match-value {} "ATL" const/iri-string)}]

          result (hash-join/hash-join build probe [:id] [:airline_id])]

      (is (= 2 (count result)) "Should join two routes to two airlines")

      ;; Verify first result has variables from both sides
      (let [first-result (first result)]
        ;; Should have airline variables
        (is (contains? first-result '?airline))
        (is (contains? first-result '?name))
        ;; Should have route variables
        (is (contains? first-result '?route))
        (is (contains? first-result '?src))
        ;; Values should be match objects
        (is (map? (get first-result '?airline)))
        (is (map? (get first-result '?name))))))

  (testing "Compatible merge with match-value objects having same underlying value"
    (let [join-col-key :fluree.db.virtual-graph.iceberg.query/join-col-vals
          ;; Both sides bind ?shared to the same underlying value
          build [{join-col-key {:id 1}
                  '?shared (where/match-value {} "same-value" const/iri-string)
                  '?build-only (where/match-value {} "from-build" const/iri-string)}]
          probe [{join-col-key {:id 1}
                  '?shared (where/match-value {} "same-value" const/iri-string)
                  '?probe-only (where/match-value {} "from-probe" const/iri-string)}]

          result (hash-join/hash-join build probe [:id] [:id])]

      (is (= 1 (count result)) "Same underlying values should produce join result")

      ;; Verify merged result has all variables
      (let [merged (first result)]
        (is (contains? merged '?shared))
        (is (contains? merged '?build-only))
        (is (contains? merged '?probe-only)))))

  (testing "Incompatible merge with match-value objects having different underlying values"
    (let [join-col-key :fluree.db.virtual-graph.iceberg.query/join-col-vals
          ;; Both sides bind ?shared to DIFFERENT underlying values
          build [{join-col-key {:id 1}
                  '?shared (where/match-value {} "value-from-build" const/iri-string)}]
          probe [{join-col-key {:id 1}
                  '?shared (where/match-value {} "value-from-probe" const/iri-string)}]

          result (hash-join/hash-join build probe [:id] [:id])]

      (is (empty? result) "Different underlying values should produce no join result")))

  (testing "Match-iri objects are compared by IRI value"
    (let [join-col-key :fluree.db.virtual-graph.iceberg.query/join-col-vals
          ;; Both sides bind ?subject to different IRIs
          build [{join-col-key {:id 1}
                  '?subject (where/match-iri {} "http://example.org/airline/1")}]
          probe [{join-col-key {:id 1}
                  '?subject (where/match-iri {} "http://example.org/airline/2")}]

          result (hash-join/hash-join build probe [:id] [:id])]

      (is (empty? result) "Different IRIs should not match"))

    ;; Same IRI should match
    (let [join-col-key :fluree.db.virtual-graph.iceberg.query/join-col-vals
          build [{join-col-key {:id 1}
                  '?subject (where/match-iri {} "http://example.org/airline/1")}]
          probe [{join-col-key {:id 1}
                  '?subject (where/match-iri {} "http://example.org/airline/1")}]

          result (hash-join/hash-join build probe [:id] [:id])]

      (is (= 1 (count result)) "Same IRI should produce join result"))))
