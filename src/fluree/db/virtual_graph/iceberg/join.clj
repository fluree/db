(ns fluree.db.virtual-graph.iceberg.join
  "Join graph construction and operations for multi-table Iceberg virtual graphs.

   This namespace provides:
   - JoinEdge data structure for representing table relationships
   - Join graph construction from R2RML RefObjectMap declarations
   - Query-time join planning utilities

   A join edge represents a foreign key relationship between two tables:
   {:child-table   \"routes\"        ; Table containing the FK
    :parent-table  \"airlines\"      ; Table containing the PK
    :columns       [{:child \"airline_id\" :parent \"id\"}]  ; Join columns (supports composite keys)
    :predicate     \"http://example.org/operatedBy\"       ; RDF predicate from RefObjectMap
    :estimated-selectivity nil}     ; Optional: for cardinality estimation

   Join Graph Structure:
   {:edges    [JoinEdge...]                    ; All join edges
    :by-table {\"table\" -> [JoinEdge...]}     ; Edges indexed by participating table
    :tm->table {\"<#TriplesMap>\" -> \"table\"}  ; TriplesMap IRI to table name lookup}"
  (:require [fluree.db.util.log :as log]))

(set! *warn-on-reflection* true)

;;; ---------------------------------------------------------------------------
;;; JoinEdge Construction
;;; ---------------------------------------------------------------------------

(defn make-join-edge
  "Create a join edge from a RefObjectMap.

   Args:
     child-table   - Table name containing the foreign key
     parent-table  - Table name containing the primary key
     join-conditions - Vector of {:child \"col\" :parent \"col\"} from R2RML
     predicate     - RDF predicate IRI from the predicateObjectMap

   Returns a join edge map."
  [child-table parent-table join-conditions predicate]
  {:child-table child-table
   :parent-table parent-table
   :columns join-conditions
   :predicate predicate
   :estimated-selectivity nil})

;;; ---------------------------------------------------------------------------
;;; Join Graph Construction
;;; ---------------------------------------------------------------------------

(defn- build-triples-map-index
  "Build an index from TriplesMap IRI to table name.

   This is needed to resolve parentTriplesMap references."
  [mappings]
  (into {}
        (for [[_table-key mapping] mappings
              :let [tm-iri (:triples-map-iri mapping)
                    table (:table mapping)]
              :when (and tm-iri table)]
          [tm-iri table])))

(defn- extract-ref-predicates
  "Extract all RefObjectMap predicates from a mapping.

   Returns a sequence of {:predicate iri :ref ref-object-map} for each
   predicate with type :ref."
  [mapping]
  (for [[pred-iri obj-map] (:predicates mapping)
        :when (= :ref (:type obj-map))]
    {:predicate pred-iri
     :ref obj-map}))

(defn build-join-graph
  "Build a join graph from R2RML mappings.

   Extracts join edges from RefObjectMap declarations in the mappings.
   Each RefObjectMap with parentTriplesMap creates a directed edge from
   the child table (containing the FK) to the parent table (containing the PK).

   Args:
     mappings - Map of {table-key -> mapping} from parse-r2rml

   Returns:
     {:edges     [JoinEdge...]
      :by-table  {\"table\" -> [JoinEdge...]}  ; All edges where table participates
      :tm->table {\"<#TriplesMap>\" -> \"table\"}}

   Example:
     Given R2RML with RouteMapping referencing AirlineMapping:
       rr:objectMap [ rr:parentTriplesMap <#AirlineMapping> ;
                      rr:joinCondition [ rr:child \"airline_id\" ; rr:parent \"id\" ] ]

     Returns edge:
       {:child-table \"routes\" :parent-table \"airlines\"
        :columns [{:child \"airline_id\" :parent \"id\"}]
        :predicate \"http://example.org/operatedBy\"}"
  [mappings]
  (let [tm->table (build-triples-map-index mappings)
        edges (vec
               (for [[_table-key mapping] mappings
                     :let [child-table (:table mapping)]
                     {:keys [predicate ref]} (extract-ref-predicates mapping)
                     :let [parent-tm (:parent-triples-map ref)
                           parent-table (get tm->table parent-tm)
                           join-conditions (:join-conditions ref)]
                     :when (and parent-table (seq join-conditions))]
                 (do
                   (log/debug "Found join edge:" {:child child-table
                                                  :parent parent-table
                                                  :predicate predicate
                                                  :columns join-conditions})
                   (make-join-edge child-table parent-table join-conditions predicate))))
        ;; Index edges by participating table (both child and parent)
        by-table (reduce (fn [idx edge]
                           (-> idx
                               (update (:child-table edge) (fnil conj []) edge)
                               (update (:parent-table edge) (fnil conj []) edge)))
                         {}
                         edges)]
    (when (seq edges)
      (log/info "Built join graph:" {:edge-count (count edges)
                                     :tables (keys by-table)}))
    {:edges edges
     :by-table by-table
     :tm->table tm->table}))

;;; ---------------------------------------------------------------------------
;;; Join Graph Query Operations
;;; ---------------------------------------------------------------------------

(defn edges-for-table
  "Get all join edges where a table participates (as child or parent)."
  [join-graph table-name]
  (get-in join-graph [:by-table table-name] []))

(defn edges-between
  "Get join edges connecting two specific tables."
  [join-graph table-a table-b]
  (let [edges-a (edges-for-table join-graph table-a)]
    (filter (fn [edge]
              (or (and (= (:child-table edge) table-a)
                       (= (:parent-table edge) table-b))
                  (and (= (:child-table edge) table-b)
                       (= (:parent-table edge) table-a))))
            edges-a)))

(defn connected-tables
  "Get all tables directly connected to a table via join edges."
  [join-graph table-name]
  (let [edges (edges-for-table join-graph table-name)]
    (set (for [edge edges]
           (if (= (:child-table edge) table-name)
             (:parent-table edge)
             (:child-table edge))))))

(defn edge-for-predicate
  "Find the join edge associated with a specific RDF predicate.

   Useful for resolving RefObjectMap predicates during query execution."
  [join-graph predicate-iri]
  (first (filter #(= (:predicate %) predicate-iri)
                 (:edges join-graph))))

(defn has-join-edges?
  "Check if the join graph has any edges."
  [join-graph]
  (boolean (seq (:edges join-graph))))

;;; ---------------------------------------------------------------------------
;;; Join Column Extraction
;;; ---------------------------------------------------------------------------

(defn child-columns
  "Extract the child column names from a join edge.

   For composite keys, returns a vector of column names."
  [edge]
  (mapv :child (:columns edge)))

(defn parent-columns
  "Extract the parent column names from a join edge.

   For composite keys, returns a vector of column names."
  [edge]
  (mapv :parent (:columns edge)))

(defn join-column-pairs
  "Get pairs of [child-col parent-col] for a join edge.

   For hash join key extraction."
  [edge]
  (mapv (juxt :child :parent) (:columns edge)))
