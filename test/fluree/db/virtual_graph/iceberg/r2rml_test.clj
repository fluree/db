(ns fluree.db.virtual-graph.iceberg.r2rml-test
  "Tests for R2RML parsing including RefObjectMap support."
  (:require [clojure.test :refer [deftest is testing]]
            [fluree.db.virtual-graph.iceberg.join :as join]
            [fluree.db.virtual-graph.iceberg.r2rml :as r2rml]))

(def test-r2rml-with-refs
  "@prefix rr: <http://www.w3.org/ns/r2rml#> .
   @prefix ex: <http://example.org/> .

   <#AirlineMapping>
       a rr:TriplesMap ;
       rr:logicalTable [ rr:tableName \"airlines\" ] ;
       rr:subjectMap [
           rr:template \"http://example.org/airline/{id}\" ;
           rr:class ex:Airline
       ] ;
       rr:predicateObjectMap [
           rr:predicate ex:name ;
           rr:objectMap [ rr:column \"name\" ]
       ] .

   <#AirportMapping>
       a rr:TriplesMap ;
       rr:logicalTable [ rr:tableName \"airports\" ] ;
       rr:subjectMap [
           rr:template \"http://example.org/airport/{id}\" ;
           rr:class ex:Airport
       ] ;
       rr:predicateObjectMap [
           rr:predicate ex:name ;
           rr:objectMap [ rr:column \"name\" ]
       ] .

   <#RouteMapping>
       a rr:TriplesMap ;
       rr:logicalTable [ rr:tableName \"routes\" ] ;
       rr:subjectMap [
           rr:template \"http://example.org/route/{id}\" ;
           rr:class ex:Route
       ] ;
       rr:predicateObjectMap [
           rr:predicate ex:code ;
           rr:objectMap [ rr:column \"code\" ]
       ] ;
       rr:predicateObjectMap [
           rr:predicate ex:operatedBy ;
           rr:objectMap [
               rr:parentTriplesMap <#AirlineMapping> ;
               rr:joinCondition [
                   rr:child \"airline_id\" ;
                   rr:parent \"id\"
               ]
           ]
       ] ;
       rr:predicateObjectMap [
           rr:predicate ex:sourceAirport ;
           rr:objectMap [
               rr:parentTriplesMap <#AirportMapping> ;
               rr:joinCondition [
                   rr:child \"src_id\" ;
                   rr:parent \"id\"
               ]
           ]
       ] .")

(deftest parse-r2rml-basic-test
  (testing "Parses basic R2RML mappings"
    (let [mappings (r2rml/parse-r2rml test-r2rml-with-refs)]
      (is (= 3 (count mappings)) "Should have 3 mappings")
      (is (contains? mappings :airlines) "Should have airlines mapping")
      (is (contains? mappings :airports) "Should have airports mapping")
      (is (contains? mappings :routes) "Should have routes mapping"))))

(deftest parse-r2rml-triples-map-iri-test
  (testing "Captures TriplesMap IRI"
    (let [mappings (r2rml/parse-r2rml test-r2rml-with-refs)
          airlines (:airlines mappings)]
      (is (some? (:triples-map-iri airlines))
          "Should have triples-map-iri")
      (is (re-find #"AirlineMapping" (:triples-map-iri airlines))
          "TriplesMap IRI should contain 'AirlineMapping'"))))

(deftest parse-r2rml-column-mapping-test
  (testing "Parses column mappings"
    (let [mappings (r2rml/parse-r2rml test-r2rml-with-refs)
          airlines (:airlines mappings)
          name-pred "http://example.org/name"
          name-map (get-in airlines [:predicates name-pred])]
      (is (= :column (:type name-map)) "Should be column type")
      (is (= "name" (:value name-map)) "Should have column name"))))

(deftest parse-r2rml-ref-object-map-test
  (testing "Parses RefObjectMap (parentTriplesMap)"
    (let [mappings (r2rml/parse-r2rml test-r2rml-with-refs)
          routes (:routes mappings)
          operated-by-pred "http://example.org/operatedBy"
          ref-map (get-in routes [:predicates operated-by-pred])]
      (is (= :ref (:type ref-map)) "Should be ref type")
      (is (some? (:parent-triples-map ref-map)) "Should have parent-triples-map")
      (is (re-find #"AirlineMapping" (:parent-triples-map ref-map))
          "Parent triples map should reference AirlineMapping"))))

(deftest parse-r2rml-join-condition-test
  (testing "Parses join conditions"
    (let [mappings (r2rml/parse-r2rml test-r2rml-with-refs)
          routes (:routes mappings)
          operated-by-pred "http://example.org/operatedBy"
          ref-map (get-in routes [:predicates operated-by-pred])
          join-conditions (:join-conditions ref-map)]
      (is (= 1 (count join-conditions)) "Should have 1 join condition")
      (is (= "airline_id" (:child (first join-conditions)))
          "Child column should be airline_id")
      (is (= "id" (:parent (first join-conditions)))
          "Parent column should be id"))))

(deftest build-join-graph-test
  (testing "Builds join graph from mappings with RefObjectMap"
    (let [mappings (r2rml/parse-r2rml test-r2rml-with-refs)
          join-graph (join/build-join-graph mappings)]
      (is (= 2 (count (:edges join-graph))) "Should have 2 join edges")
      (is (some? (:by-table join-graph)) "Should have by-table index")
      (is (some? (:tm->table join-graph)) "Should have tm->table index"))))

(deftest join-graph-edges-test
  (testing "Join edges have correct structure"
    (let [mappings (r2rml/parse-r2rml test-r2rml-with-refs)
          join-graph (join/build-join-graph mappings)
          edges (:edges join-graph)
          airline-edge (first (filter #(= "airlines" (:parent-table %)) edges))]
      (is (some? airline-edge) "Should have edge to airlines")
      (is (= "routes" (:child-table airline-edge)) "Child should be routes")
      (is (= "airlines" (:parent-table airline-edge)) "Parent should be airlines")
      (is (= [{:child "airline_id" :parent "id"}] (:columns airline-edge))
          "Should have join columns")
      (is (= "http://example.org/operatedBy" (:predicate airline-edge))
          "Should have predicate IRI"))))

(deftest join-graph-by-table-index-test
  (testing "by-table index correctly indexes edges"
    (let [mappings (r2rml/parse-r2rml test-r2rml-with-refs)
          join-graph (join/build-join-graph mappings)]
      (is (= 2 (count (get-in join-graph [:by-table "routes"])))
          "Routes should have 2 edges")
      (is (= 1 (count (get-in join-graph [:by-table "airlines"])))
          "Airlines should have 1 edge")
      (is (= 1 (count (get-in join-graph [:by-table "airports"])))
          "Airports should have 1 edge"))))

(deftest join-graph-query-operations-test
  (testing "Join graph query operations"
    (let [mappings (r2rml/parse-r2rml test-r2rml-with-refs)
          join-graph (join/build-join-graph mappings)]
      (testing "edges-for-table"
        (is (= 2 (count (join/edges-for-table join-graph "routes")))))
      (testing "edges-between"
        (is (= 1 (count (join/edges-between join-graph "routes" "airlines")))))
      (testing "connected-tables"
        (is (= #{"airlines" "airports"} (join/connected-tables join-graph "routes"))))
      (testing "edge-for-predicate"
        (let [edge (join/edge-for-predicate join-graph "http://example.org/operatedBy")]
          (is (some? edge))
          (is (= "airlines" (:parent-table edge))))))))

(deftest join-column-extraction-test
  (testing "Join column extraction helpers"
    (let [mappings (r2rml/parse-r2rml test-r2rml-with-refs)
          join-graph (join/build-join-graph mappings)
          edge (join/edge-for-predicate join-graph "http://example.org/operatedBy")]
      (is (= ["airline_id"] (join/child-columns edge)))
      (is (= ["id"] (join/parent-columns edge)))
      (is (= [["airline_id" "id"]] (join/join-column-pairs edge))))))

;; Test with composite key
(def test-r2rml-composite-key
  "@prefix rr: <http://www.w3.org/ns/r2rml#> .
   @prefix ex: <http://example.org/> .

   <#OrderMapping>
       a rr:TriplesMap ;
       rr:logicalTable [ rr:tableName \"orders\" ] ;
       rr:subjectMap [ rr:template \"http://example.org/order/{id}\" ] .

   <#OrderLineMapping>
       a rr:TriplesMap ;
       rr:logicalTable [ rr:tableName \"order_lines\" ] ;
       rr:subjectMap [ rr:template \"http://example.org/orderline/{order_id}_{line_num}\" ] ;
       rr:predicateObjectMap [
           rr:predicate ex:order ;
           rr:objectMap [
               rr:parentTriplesMap <#OrderMapping> ;
               rr:joinCondition [
                   rr:child \"order_id\" ;
                   rr:parent \"id\"
               ] ;
               rr:joinCondition [
                   rr:child \"order_version\" ;
                   rr:parent \"version\"
               ]
           ]
       ] .")

(deftest composite-key-join-test
  (testing "Composite key joins are supported"
    (let [mappings (r2rml/parse-r2rml test-r2rml-composite-key)
          join-graph (join/build-join-graph mappings)
          edges (:edges join-graph)]
      (is (= 1 (count edges)) "Should have 1 edge")
      (let [edge (first edges)]
        (is (= 2 (count (:columns edge))) "Should have 2 join columns")
        (is (= ["order_id" "order_version"] (join/child-columns edge)))
        (is (= ["id" "version"] (join/parent-columns edge)))))))
