(ns fluree.db.virtual-graph.iceberg.join-test
  "Tests for join graph construction, cardinality estimation, and join ordering."
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.virtual-graph.iceberg.join :as join]))

;;; ---------------------------------------------------------------------------
;;; Test Data: Sample Join Graph
;;; ---------------------------------------------------------------------------

(def sample-mappings
  "Sample R2RML mappings for routes, airlines, and airports tables."
  {"routes" {:table "openflights/routes"
             :triples-map-iri "<#RouteMapping>"
             :predicates {"http://example.org/operatedBy"
                          {:type :ref
                           :parent-triples-map "<#AirlineMapping>"
                           :join-conditions [{:child "airline_id" :parent "id"}]}
                          "http://example.org/sourceAirport"
                          {:type :ref
                           :parent-triples-map "<#AirportMapping>"
                           :join-conditions [{:child "src_id" :parent "id"}]}
                          "http://example.org/destAirport"
                          {:type :ref
                           :parent-triples-map "<#AirportMapping>"
                           :join-conditions [{:child "dst_id" :parent "id"}]}}}
   "airlines" {:table "openflights/airlines"
               :triples-map-iri "<#AirlineMapping>"
               :predicates {}}
   "airports" {:table "openflights/airports"
               :triples-map-iri "<#AirportMapping>"
               :predicates {}}})

(def sample-stats
  "Sample statistics for the tables."
  {"openflights/routes" {:row-count 67663
                         :column-stats {"airline_id" {:value-count 67663 :null-count 0}
                                        "src_id" {:value-count 67663 :null-count 0}
                                        "dst_id" {:value-count 67663 :null-count 0}}}
   "openflights/airlines" {:row-count 6162
                           :column-stats {"id" {:value-count 6162 :null-count 0}}}
   "openflights/airports" {:row-count 7698
                           :column-stats {"id" {:value-count 7698 :null-count 0}}}})

;;; ---------------------------------------------------------------------------
;;; Join Graph Construction Tests
;;; ---------------------------------------------------------------------------

(deftest build-join-graph-test
  (testing "Building join graph from R2RML mappings"
    (let [graph (join/build-join-graph sample-mappings)]
      (is (= 3 (count (:edges graph))) "Should have 3 edges (operatedBy, sourceAirport, destAirport)")
      (is (contains? (:by-table graph) "openflights/routes") "Routes should be indexed")
      (is (contains? (:by-table graph) "openflights/airlines") "Airlines should be indexed")
      (is (contains? (:by-table graph) "openflights/airports") "Airports should be indexed")))

  (testing "Join edges have correct structure"
    (let [graph (join/build-join-graph sample-mappings)
          airline-edge (join/edge-for-predicate graph "http://example.org/operatedBy")]
      (is (some? airline-edge) "Should find edge for operatedBy predicate")
      (is (= "openflights/routes" (:child-table airline-edge)) "Routes is child table")
      (is (= "openflights/airlines" (:parent-table airline-edge)) "Airlines is parent table")
      (is (= [{:child "airline_id" :parent "id"}] (:columns airline-edge)) "Correct join columns"))))

(deftest edges-for-table-test
  (testing "Getting edges for a table"
    (let [graph (join/build-join-graph sample-mappings)]
      (is (= 3 (count (join/edges-for-table graph "openflights/routes")))
          "Routes participates in 3 edges")
      (is (= 1 (count (join/edges-for-table graph "openflights/airlines")))
          "Airlines participates in 1 edge")
      (is (= 2 (count (join/edges-for-table graph "openflights/airports")))
          "Airports participates in 2 edges (src and dst)"))))

(deftest connected-tables-test
  (testing "Finding connected tables"
    (let [graph (join/build-join-graph sample-mappings)]
      (is (= #{"openflights/airlines" "openflights/airports"}
             (join/connected-tables graph "openflights/routes"))
          "Routes connects to airlines and airports")
      (is (= #{"openflights/routes"}
             (join/connected-tables graph "openflights/airlines"))
          "Airlines connects only to routes"))))

;;; ---------------------------------------------------------------------------
;;; Cardinality Estimation Tests
;;; ---------------------------------------------------------------------------

(deftest estimate-join-cardinality-test
  (testing "Basic cardinality estimation"
    (let [routes-stats {:row-count 67663}
          airlines-stats {:row-count 6162}]
      ;; Without NDV, assumes all unique: 67663 * 6162 / max(67663, 6162) = 6162
      (is (= 6162 (join/estimate-join-cardinality routes-stats airlines-stats
                                                  "airline_id" "id")))))

  (testing "Cardinality with NDV available"
    (let [routes-stats {:row-count 67663
                        :column-stats {"airline_id" {:distinct-count 1000}}}
          airlines-stats {:row-count 6162
                          :column-stats {"id" {:distinct-count 6162}}}]
      ;; 67663 * 6162 / max(1000, 6162) = 67663
      (is (= 67663 (join/estimate-join-cardinality routes-stats airlines-stats
                                                   "airline_id" "id")))))

  (testing "Cardinality with value-count fallback"
    (let [routes-stats {:row-count 67663
                        :column-stats {"airline_id" {:value-count 1000}}}
          airlines-stats {:row-count 6162
                          :column-stats {"id" {:value-count 6162}}}]
      ;; value-count < row-count for routes, so use 1000 as NDV
      ;; 67663 * 6162 / max(1000, 6162) = 67663
      (is (= 67663 (join/estimate-join-cardinality routes-stats airlines-stats
                                                   "airline_id" "id"))))))

(deftest estimate-selectivity-test
  (testing "No predicates = full selectivity"
    (is (= 1.0 (join/estimate-selectivity sample-stats []))))

  (testing "Equality predicate"
    (let [stats {:row-count 1000 :column-stats {"country" {:value-count 100}}}]
      ;; 1/100 = 0.01 (since value-count < row-count, NDV = 100)
      (is (< 0.009 (join/estimate-selectivity stats [{:op :eq :column "country" :value "US"}]) 0.011))))

  (testing "IN predicate"
    (let [stats {:row-count 1000 :column-stats {"status" {:value-count 5}}}]
      ;; 3/5 = 0.6
      (is (< 0.59 (join/estimate-selectivity stats [{:op :in :column "status" :value ["A" "B" "C"]}]) 0.61))))

  (testing "Range predicate"
    (let [stats {:row-count 1000}]
      ;; Default 30% for range
      (is (= 0.3 (join/estimate-selectivity stats [{:op :gt :column "amount" :value 100}])))))

  (testing "Combined predicates"
    (let [stats {:row-count 1000 :column-stats {"country" {:value-count 100}
                                                "status" {:value-count 5}}}]
      ;; 1/100 * 3/5 = 0.006
      (is (< 0.005
             (join/estimate-selectivity stats [{:op :eq :column "country" :value "US"}
                                               {:op :in :column "status" :value ["A" "B" "C"]}])
             0.007)))))

;;; ---------------------------------------------------------------------------
;;; Greedy Join Ordering Tests
;;; ---------------------------------------------------------------------------

(deftest greedy-join-order-test
  (testing "Two table join order"
    (let [graph (join/build-join-graph sample-mappings)
          tables #{"openflights/routes" "openflights/airlines"}
          order (join/greedy-join-order tables graph sample-stats {})]
      (is (= 2 (count order)) "Should have 2 tables in order")
      ;; Airlines is smaller, should come first
      (is (= "openflights/airlines" (first order)) "Smaller table (airlines) should be first")))

  (testing "Three table join order with predicates"
    (let [graph (join/build-join-graph sample-mappings)
          tables #{"openflights/routes" "openflights/airlines" "openflights/airports"}
          ;; Add predicate on routes to make it more selective
          predicates {"openflights/routes" [{:op :eq :column "airline_id" :value 123}]}
          order (join/greedy-join-order tables graph sample-stats predicates)]
      (is (= 3 (count order)) "Should have 3 tables in order")
      ;; With equality predicate on routes, it might be most selective
      (is (vector? order) "Should return a vector")))

  (testing "Join order respects connectivity"
    (let [graph (join/build-join-graph sample-mappings)
          tables #{"openflights/routes" "openflights/airlines" "openflights/airports"}
          order (join/greedy-join-order tables graph sample-stats {})]
      ;; Routes must be in the order because it's the only table connecting airlines to airports
      (is (some #{"openflights/routes"} order) "Routes must be in the join"))))

(deftest plan-join-sequence-test
  (testing "Planning join sequence"
    (let [graph (join/build-join-graph sample-mappings)
          order ["openflights/airlines" "openflights/routes"]
          plan (join/plan-join-sequence order graph sample-stats)]
      (is (= 2 (count plan)) "Should have 2 steps")
      (is (= :scan (:type (first plan))) "First step is scan")
      (is (= :hash-join (:type (second plan))) "Second step is hash-join")
      (is (some? (:edge (second plan))) "Second step has join edge")))

  (testing "Plan includes cardinality estimates"
    (let [graph (join/build-join-graph sample-mappings)
          order ["openflights/airlines" "openflights/routes"]
          plan (join/plan-join-sequence order graph sample-stats)]
      (is (number? (:estimated-rows (first plan))) "First step has estimated rows")
      (is (number? (:estimated-rows (second plan))) "Second step has estimated rows"))))
