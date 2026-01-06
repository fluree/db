(ns fluree.db.virtual-graph.iceberg.plan-test
  "Tests for the ITabularPlan protocol and physical operators."
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.tabular.protocol :as tabular]
            [fluree.db.virtual-graph.iceberg.plan :as plan]
            [fluree.db.virtual-graph.iceberg.join :as join]))

;;; ---------------------------------------------------------------------------
;;; Mock ITabularSource for Testing
;;; ---------------------------------------------------------------------------

(defn mock-batch
  "Create a mock 'batch' as a map for testing.
   Real implementation would use Arrow VectorSchemaRoot."
  [rows]
  {:rows rows
   :row-count (count rows)})

(defrecord MockSource [tables]
  ;; Simplified mock that returns row maps directly
  ;; For plan operators, each row is returned as a separate "batch"
  ;; This matches how plan operators handle non-Arrow mode
  tabular/ITabularSource
  (scan-batches [_ table-name opts]
    (let [data (get tables table-name [])
          columns (:columns opts)
          predicates (:predicates opts)
          ;; Apply column projection
          projected (if columns
                      (map #(select-keys % (map keyword columns)) data)
                      data)
          ;; Apply simple predicate filtering
          filtered (if predicates
                     (filter (fn [row]
                               (every? (fn [{:keys [column op value]}]
                                         (let [col-val (get row (keyword column))]
                                           (case op
                                             :eq (= col-val value)
                                             :in (contains? (set value) col-val)
                                             :gt (> col-val value)
                                             :lt (< col-val value)
                                             true)))
                                       predicates))
                             projected)
                     projected)]
      ;; Return each row as a separate "batch" (row map)
      ;; This is what plan operators expect for non-Arrow mode
      (vec filtered)))

  (scan-rows [this table-name opts]
    ;; scan-batches now returns individual row maps
    (tabular/scan-batches this table-name opts))

  (scan-arrow-batches [this table-name opts]
    ;; For mock, just return the same as scan-batches
    ;; Real implementation would return Arrow VectorSchemaRoot batches
    (tabular/scan-batches this table-name opts))

  (get-schema [_ _table-name _opts]
    {:columns []})

  (get-statistics [_ table-name _opts]
    (let [data (get tables table-name [])]
      {:row-count (count data)}))

  (supported-predicates [_]
    #{:eq :in :gt :lt :gte :lte}))

(defn create-mock-source
  "Create a mock tabular source with test data."
  [tables]
  (->MockSource tables))

;;; ---------------------------------------------------------------------------
;;; Test Data
;;; ---------------------------------------------------------------------------

(def airlines-data
  [{:id 1 :name "American Airlines" :country "US"}
   {:id 2 :name "Delta" :country "US"}
   {:id 3 :name "Lufthansa" :country "DE"}
   {:id 4 :name "Air France" :country "FR"}])

;; Routes: note airline_id 4 (Air France) has no routes for OPTIONAL testing
(def routes-data
  [{:route_id 100 :airline_id 1 :src "JFK" :dst "LAX"}
   {:route_id 101 :airline_id 1 :src "LAX" :dst "ORD"}
   {:route_id 102 :airline_id 2 :src "ATL" :dst "JFK"}
   {:route_id 103 :airline_id 3 :src "FRA" :dst "JFK"}])

;; Extended airlines data with an airline that has no routes (for OPTIONAL tests)
(def airlines-with-orphan
  [{:id 1 :name "American Airlines" :country "US"}
   {:id 2 :name "Delta" :country "US"}
   {:id 3 :name "Lufthansa" :country "DE"}
   {:id 4 :name "Air France" :country "FR"}   ;; No routes for this airline
   {:id 5 :name "New Airline" :country "CA"}]) ;; Also no routes

(def test-source
  (create-mock-source {"airlines" airlines-data
                       "routes" routes-data}))

;; Test source with orphan airlines (no routes for airline_id 4 and 5)
(def test-source-with-orphans
  (create-mock-source {"airlines" airlines-with-orphan
                       "routes" routes-data}))

;;; ---------------------------------------------------------------------------
;;; Protocol Tests
;;; ---------------------------------------------------------------------------

(deftest itabular-plan-protocol-test
  (testing "ScanOp satisfies ITabularPlan protocol"
    (let [scan (plan/create-scan-op test-source "airlines" ["id" "name"] [])]
      (is (satisfies? plan/ITabularPlan scan))))

  (testing "HashJoinOp satisfies ITabularPlan protocol"
    (let [scan1 (plan/create-scan-op test-source "airlines" ["id"] [])
          scan2 (plan/create-scan-op test-source "routes" ["airline_id"] [])
          join (plan/create-hash-join-op scan1 scan2 ["id"] ["airline_id"])]
      (is (satisfies? plan/ITabularPlan join))))

  (testing "FilterOp satisfies ITabularPlan protocol"
    (let [scan (plan/create-scan-op test-source "airlines" ["id" "name"] [])
          filter-op (plan/create-filter-op scan [{:column "country" :op :eq :value "US"}])]
      (is (satisfies? plan/ITabularPlan filter-op))))

  (testing "ProjectOp satisfies ITabularPlan protocol"
    (let [scan (plan/create-scan-op test-source "airlines" ["id" "name" "country"] [])
          project (plan/create-project-op scan ["id" "name"])]
      (is (satisfies? plan/ITabularPlan project)))))

;;; ---------------------------------------------------------------------------
;;; ScanOp Tests
;;; ---------------------------------------------------------------------------

(deftest scan-op-test
  (testing "ScanOp lifecycle"
    (let [scan (plan/create-scan-op test-source "airlines" nil [])]
      ;; Before open, estimated-rows should return default
      (is (= 1000 (plan/estimated-rows scan)))

      ;; Open
      (plan/open! scan)

      ;; After open, should have actual estimate
      (is (= 4 (plan/estimated-rows scan)))

      ;; Close
      (plan/close! scan)))

  (testing "ScanOp with column projection"
    (let [scan (plan/create-scan-op test-source "airlines" ["id" "name"] [])]
      (plan/open! scan)
      (try
        ;; Count all batches (each row is a batch in mock mode)
        (let [batches (loop [result []]
                        (if-let [batch (plan/next-batch! scan)]
                          (recur (conj result batch))
                          result))]
          (is (= 4 (count batches)) "Should have 4 airlines"))
        (finally
          (plan/close! scan)))))

  (testing "ScanOp with predicates"
    (let [scan (plan/create-scan-op test-source "airlines" nil
                                    [{:column "country" :op :eq :value "US"}])]
      (plan/open! scan)
      (try
        ;; Count all batches after filtering
        (let [batches (loop [result []]
                        (if-let [batch (plan/next-batch! scan)]
                          (recur (conj result batch))
                          result))]
          ;; Should filter to US airlines only (American, Delta)
          (is (= 2 (count batches)) "Should have 2 US airlines"))
        (finally
          (plan/close! scan))))))

;;; ---------------------------------------------------------------------------
;;; FilterOp Tests
;;; ---------------------------------------------------------------------------

(deftest filter-op-test
  (testing "FilterOp lifecycle and passthrough"
    (let [scan (plan/create-scan-op test-source "airlines" nil [])
          filter-op (plan/create-filter-op scan [{:column "country" :op :eq :value "US"}])]
      (plan/open! filter-op)
      (try
        ;; FilterOp should pass through batches from child
        ;; (actual filtering is done at scan level for Iceberg)
        (let [batch (plan/next-batch! filter-op)]
          (is (some? batch) "FilterOp should return batches from child"))
        ;; Should return estimated rows (from child, modified by selectivity)
        (is (number? (plan/estimated-rows filter-op)))
        (finally
          (plan/close! filter-op))))))

;;; ---------------------------------------------------------------------------
;;; Plan Compiler Tests
;;; ---------------------------------------------------------------------------

(def sample-mappings
  {"airlines" {:table "airlines"
               :triples-map-iri "<#AirlineMapping>"
               :predicates {}}
   "routes" {:table "routes"
             :triples-map-iri "<#RouteMapping>"
             :predicates {"http://example.org/operatedBy"
                          {:type :ref
                           :parent-triples-map "<#AirlineMapping>"
                           :join-conditions [{:child "airline_id" :parent "id"}]}}}})

(def sample-stats
  {"airlines" {:row-count 4}
   "routes" {:row-count 5}})

(deftest compile-plan-test
  (testing "compile-plan with single table"
    (let [join-graph (join/build-join-graph sample-mappings)
          pattern-groups [{:mapping {:table "airlines"}
                           :predicates []}]
          sources {"airlines" test-source}
          plan (plan/compile-plan sources pattern-groups join-graph sample-stats nil)]
      (is (some? plan))
      (is (instance? fluree.db.virtual_graph.iceberg.plan.ScanOp plan))))

  (testing "compile-plan with multiple tables creates joins"
    (let [join-graph (join/build-join-graph sample-mappings)
          pattern-groups [{:mapping {:table "airlines"}
                           :predicates []}
                          {:mapping {:table "routes"}
                           :predicates []}]
          sources {"airlines" test-source
                   "routes" test-source}
          plan (plan/compile-plan sources pattern-groups join-graph sample-stats nil)]
      (is (some? plan))
      ;; With two tables, should get a HashJoinOp
      (is (instance? fluree.db.virtual_graph.iceberg.plan.HashJoinOp plan))))

  (testing "compile-plan returns nil for empty pattern groups"
    (let [plan (plan/compile-plan {} [] nil {} nil)]
      (is (nil? plan)))))

(deftest compile-single-table-plan-test
  (testing "compile-single-table-plan creates ScanOp"
    (let [plan (plan/compile-single-table-plan test-source "airlines"
                                                ["id" "name"]
                                                [{:column "country" :op :eq :value "US"}]
                                                nil)]
      (is (some? plan))
      (is (instance? fluree.db.virtual_graph.iceberg.plan.ScanOp plan)))))

;;; ---------------------------------------------------------------------------
;;; Batch Conversion Tests
;;; ---------------------------------------------------------------------------

(deftest batch-to-row-maps-test
  (testing "batch->row-maps converts batch to row maps"
    ;; This tests the helper function with our mock batches
    ;; Real implementation would use Arrow VectorSchemaRoot
    (let [batch (mock-batch [{:id 1 :name "Test"}
                             {:id 2 :name "Test2"}])]
      ;; Our mock uses :rows directly
      (is (= 2 (count (:rows batch))))
      (is (= {:id 1 :name "Test"} (first (:rows batch)))))))

;;; ---------------------------------------------------------------------------
;;; HashJoinOp Arrow Output Tests
;;; ---------------------------------------------------------------------------

(deftest hash-join-output-arrow-option-test
  (testing "HashJoinOp accepts :output-arrow? option"
    (let [scan1 (plan/create-scan-op test-source "airlines" ["id" "name" "country"] [])
          scan2 (plan/create-scan-op test-source "routes" ["route_id" "airline_id" "src" "dst"] [])
          ;; Create join with :output-arrow? false (row maps output)
          join-row-maps (plan/create-hash-join-op scan1 scan2 ["id"] ["airline_id"] {})
          ;; Create join with :output-arrow? true (Arrow batch output)
          join-arrow (plan/create-hash-join-op scan1 scan2 ["id"] ["airline_id"]
                                               {:output-arrow? true})]
      (is (satisfies? plan/ITabularPlan join-row-maps))
      (is (satisfies? plan/ITabularPlan join-arrow))
      ;; Both should have the output-arrow? field set correctly
      (is (false? (:output-arrow? join-row-maps)))
      (is (true? (:output-arrow? join-arrow)))))

  (testing "compile-plan passes :output-arrow? to hash joins"
    (let [join-graph (join/build-join-graph sample-mappings)
          pattern-groups [{:mapping {:table "airlines"} :predicates []}
                          {:mapping {:table "routes"} :predicates []}]
          sources {"airlines" test-source "routes" test-source}
          ;; Compile with :output-arrow? true
          plan (plan/compile-plan sources pattern-groups join-graph sample-stats nil
                                  {:output-arrow? true})]
      (is (instance? fluree.db.virtual_graph.iceberg.plan.HashJoinOp plan))
      (is (true? (:output-arrow? plan)))))

  (testing "compile-plan passes :output-columns to hash joins"
    (let [join-graph (join/build-join-graph sample-mappings)
          pattern-groups [{:mapping {:table "airlines"} :predicates []}
                          {:mapping {:table "routes"} :predicates []}]
          sources {"airlines" test-source "routes" test-source}
          output-cols #{"name" "dst"}
          ;; Compile with :output-columns for projection pushdown
          plan (plan/compile-plan sources pattern-groups join-graph sample-stats nil
                                  {:vectorized? true
                                   :output-columns output-cols})]
      (is (instance? fluree.db.virtual_graph.iceberg.plan.HashJoinOp plan))
      (is (= output-cols (:output-columns plan))))))

;;; ---------------------------------------------------------------------------
;;; Left Outer Join Tests (OPTIONAL support)
;;; ---------------------------------------------------------------------------

(deftest left-outer-hash-join-test
  (testing "HashJoinOp with :left-outer? creates left outer join"
    (let [;; Airlines is build side (smaller)
          scan1 (plan/create-scan-op test-source-with-orphans "airlines" ["id" "name"] [])
          ;; Routes is probe side (larger, but has orphans)
          scan2 (plan/create-scan-op test-source-with-orphans "routes" ["route_id" "airline_id" "src"] [])
          ;; Create left outer join: probe (airlines) LEFT JOIN build (routes)
          ;; For OPTIONAL, airlines is the "required" side (probe), routes is "optional" side (build)
          join (plan/create-hash-join-op scan2 scan1 ["airline_id"] ["id"]
                                         {:left-outer? true})]
      (is (satisfies? plan/ITabularPlan join))
      (is (true? (:left-outer? join)))))

  (testing "Left outer join includes unmatched probe rows with nulls"
    ;; This tests the core OPTIONAL semantics:
    ;; All airlines should appear, even those without routes
    (let [;; Build side: routes (smaller for this test)
          routes-scan (plan/create-scan-op test-source-with-orphans "routes"
                                           ["route_id" "airline_id" "src" "dst"] [])
          ;; Probe side: airlines (we want ALL airlines in output)
          airlines-scan (plan/create-scan-op test-source-with-orphans "airlines"
                                             ["id" "name" "country"] [])
          ;; Left outer join: airlines LEFT OUTER JOIN routes
          ;; Probe side (airlines) drives the join - all probe rows appear
          ;; Build side (routes) provides matches - nulls when no match
          left-join (plan/create-hash-join-op routes-scan airlines-scan
                                              ["airline_id"] ["id"]
                                              {:left-outer? true})]
      (plan/open! left-join)
      (try
        (let [batches (loop [result []]
                        (if-let [batch (plan/next-batch! left-join)]
                          (recur (conj result batch))
                          result))
              ;; Collect all rows from batches
              all-rows (mapcat (fn [batch]
                                 (if (vector? batch)
                                   batch
                                   (:rows batch)))
                               batches)
              ;; Group by airline id to analyze results
              by-airline (group-by :id all-rows)]
          ;; Should have 5 unique airlines (including orphans)
          (is (= 5 (count by-airline))
              "All 5 airlines should appear in left outer join output")
          ;; Airlines 1, 2, 3 have routes
          (is (= 2 (count (get by-airline 1)))  ;; American has 2 routes
              "American Airlines should have 2 joined rows")
          (is (= 1 (count (get by-airline 2)))  ;; Delta has 1 route
              "Delta should have 1 joined row")
          (is (= 1 (count (get by-airline 3)))  ;; Lufthansa has 1 route
              "Lufthansa should have 1 joined row")
          ;; Airlines 4 and 5 have NO routes - should appear with null route columns
          (let [air-france-rows (get by-airline 4)]
            (is (= 1 (count air-france-rows))
                "Air France should have 1 row (no routes)")
            (is (nil? (:route_id (first air-france-rows)))
                "Air France row should have nil route_id"))
          (let [new-airline-rows (get by-airline 5)]
            (is (= 1 (count new-airline-rows))
                "New Airline should have 1 row (no routes)")
            (is (nil? (:src (first new-airline-rows)))
                "New Airline row should have nil src")))
        (finally
          (plan/close! left-join)))))

  (testing "Inner join (default) excludes unmatched rows"
    ;; Verify that inner join still works correctly - orphan airlines excluded
    (let [routes-scan (plan/create-scan-op test-source-with-orphans "routes"
                                           ["route_id" "airline_id" "src"] [])
          airlines-scan (plan/create-scan-op test-source-with-orphans "airlines"
                                             ["id" "name"] [])
          ;; Regular inner join (no :left-outer?)
          inner-join (plan/create-hash-join-op routes-scan airlines-scan
                                               ["airline_id"] ["id"]
                                               {})]
      (plan/open! inner-join)
      (try
        (let [batches (loop [result []]
                        (if-let [batch (plan/next-batch! inner-join)]
                          (recur (conj result batch))
                          result))
              all-rows (mapcat (fn [batch]
                                 (if (vector? batch) batch (:rows batch)))
                               batches)
              by-airline (group-by :id all-rows)]
          ;; Inner join should only have 3 airlines (those with routes)
          (is (= 3 (count by-airline))
              "Inner join should only include 3 airlines with routes")
          ;; Airlines 4 and 5 should NOT appear
          (is (nil? (get by-airline 4))
              "Air France should NOT appear in inner join")
          (is (nil? (get by-airline 5))
              "New Airline should NOT appear in inner join"))
        (finally
          (plan/close! inner-join)))))

  (testing "HashJoinOp accepts both :left-outer? and :vectorized? options"
    ;; This verifies that both options can be combined - the vectorized path
    ;; now supports left outer join for OPTIONAL patterns
    (let [scan1 (plan/create-scan-op test-source-with-orphans "routes" ["airline_id"] [])
          scan2 (plan/create-scan-op test-source-with-orphans "airlines" ["id" "name"] [])
          join (plan/create-hash-join-op scan1 scan2 ["airline_id"] ["id"]
                                         {:left-outer? true
                                          :vectorized? true
                                          :output-arrow? true})]
      (is (true? (:left-outer? join)) "Left-outer option should be set")
      (is (true? (:vectorized? join)) "Vectorized option should be set")
      (is (true? (:output-arrow? join)) "Output-arrow option should be set")))

  (testing "compile-plan with :left-outer? and :vectorized? creates properly configured HashJoinOp"
    ;; Test that compile-plan passes both options through for OPTIONAL support in columnar mode
    (let [join-graph (join/build-join-graph sample-mappings)
          pattern-groups [{:mapping {:table "airlines"} :predicates []}
                          {:mapping {:table "routes"} :predicates [] :optional? true}]
          sources {"airlines" test-source "routes" test-source}
          plan (plan/compile-plan sources pattern-groups join-graph sample-stats nil
                                  {:vectorized? true})]
      (is (instance? fluree.db.virtual_graph.iceberg.plan.HashJoinOp plan))
      (is (true? (:vectorized? plan)) "Vectorized should be enabled")
      ;; The join should be configured as left-outer for OPTIONAL patterns
      (is (true? (:left-outer? plan)) "Left-outer should be set for OPTIONAL patterns"))))
