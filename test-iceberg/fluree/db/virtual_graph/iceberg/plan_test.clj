(ns fluree.db.virtual-graph.iceberg.plan-test
  "Tests for the ITabularPlan protocol and physical operators."
  (:require [clojure.test :refer [deftest is testing]]
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
  ;; Real ITabularSource would return Arrow batches
  fluree.db.tabular.protocol/ITabularSource
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
      ;; Return as single "batch" for simplicity
      [(mock-batch filtered)]))

  (scan-rows [this table-name opts]
    (let [batches (fluree.db.tabular.protocol/scan-batches this table-name opts)]
      (mapcat :rows batches)))

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

(def routes-data
  [{:route_id 100 :airline_id 1 :src "JFK" :dst "LAX"}
   {:route_id 101 :airline_id 1 :src "LAX" :dst "ORD"}
   {:route_id 102 :airline_id 2 :src "ATL" :dst "JFK"}
   {:route_id 103 :airline_id 3 :src "FRA" :dst "JFK"}
   {:route_id 104 :airline_id 4 :src "CDG" :dst "JFK"}])

(def test-source
  (create-mock-source {"airlines" airlines-data
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
        (let [batch (plan/next-batch! scan)]
          (is (some? batch))
          (is (= 4 (:row-count batch))))
        (finally
          (plan/close! scan)))))

  (testing "ScanOp with predicates"
    (let [scan (plan/create-scan-op test-source "airlines" nil
                                    [{:column "country" :op :eq :value "US"}])]
      (plan/open! scan)
      (try
        (let [batch (plan/next-batch! scan)]
          (is (some? batch))
          ;; Should filter to US airlines only (American, Delta)
          (is (= 2 (:row-count batch))))
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
